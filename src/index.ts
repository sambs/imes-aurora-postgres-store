import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

import { EqualFilter, Item, ItemKey, QueryableStore, QueryResult } from 'imes'

export abstract class AuroraPostgresIndex<I extends Item<any, any, any>, T> {
  getValue: (item: I) => T

  constructor(getValue: (item: I) => T) {
    this.getValue = getValue
  }

  parameterFromItem(name: string, item: I): RDSDataService.SqlParameter {
    const value = this.getValue(item)
    return { name: name, value: this.toParameterValue(value) }
  }

  abstract toParameterValue(value: T): RDSDataService.Field
}

export class AuroraPostgresStringIndex<
  I extends Item<any, any, any>
> extends AuroraPostgresIndex<I, string> {
  toParameterValue(value: string) {
    return { stringValue: value }
  }
}

type AuroraPostgresIndexes<I extends Item<any, any, any>> = {
  [name: string]: AuroraPostgresIndex<I, any>
}

type AuroraPostgresQuery<
  I extends Item<any, any, any>,
  X extends AuroraPostgresIndexes<I>
> = {
  cursor?: ItemKey<I>
  limit?: number
  filter?: {
    [name in keyof X]?: X[name] extends AuroraPostgresStringIndex<I>
      ? EqualFilter<string>
      : never
  }
}

export interface AuroraPostgresStoreOptions<
  I extends Item<any, any, string>,
  X extends AuroraPostgresIndexes<I>
> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  indexes: X
}

export class AuroraPostgresStore<
  I extends Item<any, any, string>,
  X extends AuroraPostgresIndexes<I>
> implements QueryableStore<I, AuroraPostgresQuery<I, X>> {
  client: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  indexes: X

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
    indexes,
  }: AuroraPostgresStoreOptions<I, X>) {
    this.client = client || new RDSDataService()
    this.clusterArn = clusterArn
    this.database = database
    this.secretArn = secretArn
    this.table = table
    this.indexes = indexes
  }

  async get(key: ItemKey<I>): Promise<I | undefined> {
    const result = await this.client
      .executeStatement({
        sql: `SELECT item FROM ${this.table} WHERE id = :id`,
        parameters: [{ name: 'id', value: { stringValue: key } }],
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()

    if (result.records?.length) {
      const json = result.records[0][0].stringValue
      if (json) return JSON.parse(json)
    }
  }

  async create(item: I): Promise<void> {
    let fields = ['id', 'item']
    let values = [':id, :item::jsonb']
    let parameters: RDSDataService.SqlParametersList = [
      { name: 'id', value: { stringValue: item.key } },
      { name: 'item', value: { stringValue: JSON.stringify(item) } },
    ]

    for (const name in this.indexes) {
      const index = this.indexes[name]
      fields.push(name)
      values.push(`:${name}`)
      parameters.push(index.parameterFromItem(name, item))
    }

    await this.client
      .executeStatement({
        sql: `
  INSERT INTO ${this.table} (${fields.join(', ')})
  VALUES(${values.join(', ')})
`,
        parameters,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }

  async update(item: I): Promise<void> {
    let set = ['item = :item::jsonb']
    let parameters: RDSDataService.SqlParametersList = [
      { name: 'id', value: { stringValue: item.key } },
      { name: 'item', value: { stringValue: JSON.stringify(item) } },
    ]

    for (const name in this.indexes) {
      const index = this.indexes[name]
      set.push(`${name} = :${name}`)
      parameters.push(index.parameterFromItem(name, item))
    }

    await this.client
      .executeStatement({
        sql: `
  UPDATE ${this.table}
  SET ${set.join(', ')}
  WHERE id = :id
`,
        parameters,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }

  async find(query: AuroraPostgresQuery<I, X>): Promise<QueryResult<I>> {
    const hasLimit = typeof query.limit == 'number'
    let sql = `SELECT id,item FROM ${this.table}`
    let where: string[] = []
    let parameters: RDSDataService.SqlParametersList = []

    if (query.cursor) {
      where = [...where, 'id > :id']
      parameters = [
        ...parameters,
        { name: 'id', value: { stringValue: query.cursor } },
      ]
    }

    if (query.filter) {
      for (const name in this.indexes) {
        if (name in query.filter) {
          const index = this.indexes[name]
          const filter = query.filter[name]
          if (filter && filter.eq) {
            where.push(`${name} = :${name}`)
            parameters.push({
              name,
              value: index.toParameterValue(filter.eq),
            })
          }
        }
      }
    }

    if (where.length) {
      sql = `${sql} WHERE ${where.join(' AND ')}`
    }

    sql = `${sql} ORDER BY id`

    if (hasLimit) {
      sql = `${sql} LIMIT ${query.limit! + 1}`
    }

    let { records = [] } = await this.client
      .executeStatement({
        sql,
        parameters,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()

    const cursor =
      hasLimit && records.length > query.limit!
        ? records[query.limit! - 1][0].stringValue!
        : null

    if (hasLimit) {
      records = records.slice(0, query.limit!)
    }

    return {
      cursor: cursor as ItemKey<I>,
      items: records.map(([_id, { stringValue }]) => JSON.parse(stringValue!)),
    }
  }
}
