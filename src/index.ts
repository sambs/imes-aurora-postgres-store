import { Item, ItemKey, Query, QueryableStore, QueryResult } from 'imes'
import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

export abstract class AuroraPostgresIndex<
  I extends Item<any, any, any>,
  N extends string,
  T
> {
  name: N
  getValue: (item: I) => T

  constructor(name: N, getValue: (item: I) => T) {
    this.name = name
    this.getValue = getValue
  }

  abstract toParameter(value: I): RDSDataService.SqlParametersList[number]
}

export class AuroraPostgresStringIndex<
  I extends Item<any, any, any>,
  N extends string
> extends AuroraPostgresIndex<I, N, string> {
  toParameter(item: I) {
    const value = this.getValue(item)
    return { name: this.name, value: { stringValue: value } }
  }
}

export interface AuroraPostgresStoreOptions<
  _I extends Item<any, any, string>,
  _Q
> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  indexes?: AuroraPostgresIndex<any, any, any>[]
}

export class AuroraPostgresStore<
  I extends Item<any, any, string>,
  Q extends Query<I>
> implements QueryableStore<I, Q> {
  client: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  indexes: AuroraPostgresIndex<any, any, any>[]

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
    indexes = [],
  }: AuroraPostgresStoreOptions<I, Q>) {
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

    for (const index of this.indexes) {
      fields.push(index.name)
      values.push(`:${index.name}`)
      parameters.push(index.toParameter(item))
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

    for (const index of this.indexes) {
      set.push(`${index.name} = :${index.name}`)
      parameters.push(index.toParameter(item))
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

  async find(query: Q): Promise<QueryResult<I>> {
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

    if (where.length) {
      sql = `${sql} WHERE ${where.join(' AND ')}`
    }

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
