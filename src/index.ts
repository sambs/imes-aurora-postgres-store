import { Item, ItemKey, Query, QueryableStore, QueryResult } from 'imes'
import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

export interface AuroraPostgresStoreOptions<
  _I extends Item<any, any, any>,
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

  async read(key: ItemKey<I>): Promise<I | undefined> {
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

  async write(item: I): Promise<void> {
    await this.client
      .executeStatement({
        sql: `
  INSERT INTO ${this.table} (id, item)
  VALUES(:id, :item::jsonb)
  ON CONFLICT (id) DO UPDATE SET item = :item::jsonb
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

  async find(_query: Q): Promise<QueryResult<I>> {
    return {
      items: [],
      cursor: null,
    }
  }
}
