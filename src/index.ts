import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

import { ExactFilter, OrdFilter, Query, QueryResult, Store } from 'imes'

type Filter = {}

export abstract class AuroraPostgresIndex<I, T, F extends Filter> {
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

export class AuroraPostgresExactIndex<I, T> extends AuroraPostgresIndex<
  I,
  T,
  ExactFilter<T>
> {
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

  filterParams(name: string, filter: ExactFilter<T>) {
    const where: string[] = []
    const parameters: RDSDataService.SqlParametersList = []

    if (filter.eq !== undefined) {
      const paramName = `${name}__eq`
      where.push(`${name} = :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.eq),
      })
    }

    if (filter.ne !== undefined) {
      const paramName = `${name}__ne`
      where.push(`${name} <> :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.ne),
      })
    }

    if (filter.in !== undefined) {
      // Aurora data api does not currently support array parameters
      // but we can add items individually
      const paramNames: string[] = []

      filter.in.forEach((value, index) => {
        const paramName = `${name}__in_${index}`
        paramNames.push(paramName)
        parameters.push({
          name: paramName,
          value: this.parameterValue(value),
        })
      })

      where.push(`${name} IN (:${paramNames.join(', :')})`)
    }

    return { where, parameters }
  }
}

export class AuroraPostgresOrdIndex<I, T> extends AuroraPostgresIndex<
  I,
  T,
  OrdFilter<T>
> {
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

  filterParams(name: string, filter: OrdFilter<T>) {
    const where: string[] = []
    const parameters: RDSDataService.SqlParametersList = []

    if (filter.eq !== undefined) {
      const paramName = `${name}__eq`
      where.push(`${name} = :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.eq),
      })
    }

    if (filter.gt !== undefined) {
      const paramName = `${name}_gt`
      where.push(`${name} > :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.gt),
      })
    }

    if (filter.gte !== undefined) {
      const paramName = `${name}_gte`
      where.push(`${name} >= :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.gte),
      })
    }

    if (filter.lt !== undefined) {
      const paramName = `${name}_lt`
      where.push(`${name} < :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.lt),
      })
    }

    if (filter.lte !== undefined) {
      const paramName = `${name}_lte`
      where.push(`${name} <= :${paramName}`)
      parameters.push({
        name: paramName,
        value: this.parameterValue(filter.lte),
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

type AuroraPostgresIndexes<I, Q extends Query, F = Required<Q['filter']>> = {
  [name in keyof F]: AuroraPostgresIndex<I, any, F[name]>
}

export interface AuroraPostgresStoreOptions<I, Q extends Query> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  getKey: (item: I) => string
  indexes: AuroraPostgresIndexes<I, Q>
}

export class AuroraPostgresStore<I, Q extends Query>
  implements Store<I, string, Q> {
  client: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  getKey: (item: I) => string
  indexes: AuroraPostgresIndexes<I, Q>

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
    getKey,
    indexes,
  }: AuroraPostgresStoreOptions<I, Q>) {
    this.client = client || new RDSDataService()
    this.clusterArn = clusterArn
    this.database = database
    this.secretArn = secretArn
    this.table = table
    this.getKey = getKey
    this.indexes = indexes
  }

  async get(key: string): Promise<I | undefined> {
    const result = await this.client
      .executeStatement({
        sql: `SELECT item FROM ${this.table} WHERE key = :key`,
        parameters: [{ name: 'key', value: { stringValue: key } }],
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
    let fields = ['key', 'item']
    let values = [':key, :item::jsonb']
    let parameters: RDSDataService.SqlParametersList = [
      { name: 'key', value: { stringValue: this.getKey(item) } },
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
      { name: 'key', value: { stringValue: this.getKey(item) } },
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
  WHERE key = :key
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
    let sql = `SELECT key,item FROM ${this.table}`
    let where: string[] = []
    let parameters: RDSDataService.SqlParametersList = []

    if (query.cursor) {
      where = [...where, 'key > :key']
      parameters = [
        ...parameters,
        { name: 'key', value: { stringValue: query.cursor } },
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

    sql = `${sql} ORDER BY key`

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
      cursor,
      items: records.map(([_key, { stringValue }]) => JSON.parse(stringValue!)),
    }
  }

  async setup() {
    await this.client
      .executeStatement({
        sql: `CREATE TABLE ${this.table} (key varchar(64) PRIMARY KEY, item jsonb)`,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }

  async teardown() {
    await this.client
      .executeStatement({
        sql: `DROP TABLE ${this.table}`,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }

  async clear() {
    await this.client
      .executeStatement({
        sql: `DELETE FROM ${this.table}`,
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }
}
