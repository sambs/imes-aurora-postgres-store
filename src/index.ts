import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { Query, QueryFilterField, QueryResult, Store } from 'imes'

export type AuroraPostgresIndex<I, T> = {
  value: (value: T) => RDSDataService.Field
  typeHint?: RDSDataService.TypeHint
  pick: (item: I) => T
}

export type AuroraPostgresIndexes<I> = {
  [name: string]: AuroraPostgresIndex<I, any>
}

export type AuroraPostgresFilters<
  Q extends Query,
  F = Required<Q['filter']>
> = {
  [name in keyof F]: AuroraPostgresFilterField<F[name]>
}

export type AuroraPostgresFilterField<F extends QueryFilterField> = {
  [comparator in keyof Required<F>]: AuroraPostgresFilterClause<
    Exclude<F[comparator], undefined>
  >
}

export type AuroraPostgresFilterClause<T> = (
  value: T
) => {
  where: string
  parameters: RDSDataService.SqlParameter[]
}

export interface AuroraPostgresStoreOptions<I, Q extends Query> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  getItemKey: (item: I) => string
  indexes: AuroraPostgresIndexes<I>
  filters: AuroraPostgresFilters<Q>
}

export class AuroraPostgresStore<I, Q extends Query>
  implements Store<I, string, Q> {
  client: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
  getItemKey: (item: I) => string
  indexes: AuroraPostgresIndexes<I>
  filters: AuroraPostgresFilters<Q>

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
    getItemKey,
    indexes,
    filters,
  }: AuroraPostgresStoreOptions<I, Q>) {
    this.client = client || new RDSDataService()
    this.clusterArn = clusterArn
    this.database = database
    this.secretArn = secretArn
    this.table = table
    this.getItemKey = getItemKey
    this.indexes = indexes
    this.filters = filters
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
    let columns = ['key', 'item']
    let values = [':key, :item::jsonb']
    let parameters: RDSDataService.SqlParametersList = [
      { name: 'key', value: { stringValue: this.getItemKey(item) } },
      { name: 'item', value: { stringValue: JSON.stringify(item) } },
    ]

    for (const name in this.indexes) {
      const index = this.indexes[name]
      columns.push(name)
      values.push(`:${name}`)
      parameters.push({
        name,
        value: index.value(index.pick(item)),
        typeHint: index.typeHint,
      })
    }

    await this.client
      .executeStatement({
        sql: `
  INSERT INTO ${this.table} (${columns.map(name => `"${name}"`).join(', ')})
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
      { name: 'key', value: { stringValue: this.getItemKey(item) } },
      { name: 'item', value: { stringValue: JSON.stringify(item) } },
    ]

    for (const name in this.indexes) {
      const index = this.indexes[name]
      set.push(`"${name}" = :${name}`)
      parameters.push({
        name,
        value: index.value(index.pick(item)),
        typeHint: index.typeHint,
      })
    }

    await this.client
      .executeStatement({
        // sql: '',
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
      for (const field in this.filters) {
        if (query.filter[field]) {
          for (const comparator in this.filters[field]) {
            if (query.filter[field]![comparator] !== undefined) {
              const filterValue = query.filter[field]![comparator]
              const params = this.filters[field][comparator](filterValue)
              where = [...where, params.where]
              parameters = [...parameters, ...params.parameters]
            }
          }
        }
      }
    }

    if (where.length) {
      sql = `${sql} WHERE ${where.join(' AND ')}`
    }

    sql = `${sql} ORDER BY key`

    if (hasLimit) {
      // Fetch one extra record to determine if there are more records after
      // the amount requested
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

    const edges = records.map(
      ([{ stringValue: key }, { stringValue: item }]) => ({
        cursor: key!,
        node: JSON.parse(item!),
      })
    )

    const items = edges.map(({ node }) => node)

    return { cursor, edges, items }
  }

  async setup() {
    await this.client
      .executeStatement({
        sql: `CREATE TABLE ${this.table} ("key" varchar(64) PRIMARY KEY, "item" jsonb)`,
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

export * from './values'
export * from './filters'
