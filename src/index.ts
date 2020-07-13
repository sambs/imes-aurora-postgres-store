import { Item, ItemKey, Query, QueryableStore, QueryResult } from 'imes'
import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

export interface AuroraPostgresStoreOptions<
  _I extends Item<any, any, string>,
  _Q
> {
  client?: RDSDataService
  clusterArn: string
  database: string
  secretArn: string
  table: string
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

  constructor({
    client,
    clusterArn,
    database,
    table,
    secretArn,
  }: AuroraPostgresStoreOptions<I, Q>) {
    this.client = client || new RDSDataService()
    this.clusterArn = clusterArn
    this.database = database
    this.secretArn = secretArn
    this.table = table
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
    await this.client
      .executeStatement({
        sql: `
  INSERT INTO ${this.table} (id, item)
  VALUES(:id, :item::jsonb)
`,
        parameters: [
          { name: 'id', value: { stringValue: item.key } },
          { name: 'item', value: { stringValue: JSON.stringify(item) } },
        ],
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
      })
      .promise()
  }

  async update(item: I): Promise<void> {
    await this.client
      .executeStatement({
        sql: `
  UPDATE ${this.table}
  SET item = :item::jsonb
  WHERE id = :id
`,
        parameters: [
          { name: 'id', value: { stringValue: item.key } },
          { name: 'item', value: { stringValue: JSON.stringify(item) } },
        ],
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
