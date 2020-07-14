import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

import {
  EqualFilter,
  Item,
  ItemKey,
  Query,
  QueryableStore,
  QueryResult,
} from 'imes'

type Filter = {}

export abstract class AuroraPostgresIndex<
  I extends Item<any, any, any>,
  T,
  F extends Filter
> {
  valueFromItem: (item: I) => T
  parameterValue: (value: T) => RDSDataService.Field

  constructor(
    parameterValue: (value: T) => RDSDataService.Field,
    valueFromItem: (item: I) => T
  ) {
    this.parameterValue = parameterValue
    this.valueFromItem = valueFromItem
  }

  parameterFromItem(name: string, item: I): RDSDataService.SqlParameter {
    const value = this.valueFromItem(item)
    return { name: name, value: this.parameterValue(value) }
  }

  abstract createParams(
    name: string,
    item: I
  ): {
    fields: string[]
    values: string[]
    parameters: RDSDataService.SqlParametersList
  }

  abstract updateParams(
    name: string,
    item: I
  ): {
    set: string[]
    parameters: RDSDataService.SqlParametersList
  }

  abstract filterParams(
    name: string,
    filter: F
  ): {
    where: string[]
    parameters: RDSDataService.SqlParametersList
  }
}

export class AuroraPostgresEqualIndex<
  I extends Item<any, any, any>,
  T
> extends AuroraPostgresIndex<I, T, EqualFilter<T>> {
  createParams(name: string, item: I) {
    return {
      fields: [name],
      values: [`:${name}`],
      parameters: [this.parameterFromItem(name, item)],
    }
  }

  updateParams(name: string, item: I) {
    return {
      set: [`${name} = :${name}`],
      parameters: [this.parameterFromItem(name, item)],
    }
  }

  filterParams(name: string, filter: EqualFilter<T>) {
    const where: string[] = []
    const parameters: RDSDataService.SqlParametersList = []

    if (filter.eq) {
      const paramName = `${name}__eq`
      where.push(`${name} = :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.eq),
      })
    }

    if (filter.ne) {
      const paramName = `${name}__ne`
      where.push(`${name} = :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.ne),
      })
    }

    return { where, parameters }
  }
}

export const auroraStringValue = (value: string): RDSDataService.Field => ({
  stringValue: value,
})

export const auroraLongValue = (value: number): RDSDataService.Field => ({
  longValue: value,
})

export const auroraDoubleValue = (value: number): RDSDataService.Field => ({
  doubleValue: value,
})

export const auroraBooleanValue = (value: boolean): RDSDataService.Field => ({
  booleanValue: value,
})

export const auroraNullable = <T>(
  higher: (value: T) => RDSDataService.Field
) => (value: T | null) => {
  if (value === null) return { isNull: true }
  else return higher(value)
}

type AuroraPostgresIndexes<
  I extends Item<any, any, string>,
  Q extends Query<I>,
  F = Required<Q['filter']>
> = {
  [name in keyof F]: AuroraPostgresIndex<I, any, F[name]>
}

export interface AuroraPostgresStoreOptions<
  I extends Item<any, any, string>,
  Q extends Query<I>
> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  indexes: AuroraPostgresIndexes<I, Q>
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
  indexes: AuroraPostgresIndexes<I, Q>

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
    indexes,
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

    for (const name in this.indexes) {
      const params = this.indexes[name].createParams(name, item)
      fields = [...fields, ...params.fields]
      values = [...values, ...params.values]
      parameters = [...parameters, ...params.parameters]
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
      const params = this.indexes[name].updateParams(name, item)
      set = [...set, ...params.set]
      parameters = [...parameters, ...params.parameters]
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

    if (query.filter) {
      for (const name in this.indexes) {
        if (name in query.filter) {
          const index = this.indexes[name]
          const filter = query.filter[name]
          if (filter) {
            const params = index.filterParams(name, filter)
            where = [...where, ...params.where]
            parameters = [...parameters, ...params.parameters]
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
