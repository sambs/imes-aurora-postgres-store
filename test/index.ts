import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { OrdFilter, EqualFilter, PrefixFilter, Query } from 'imes'
import { AuroraPostgresStore } from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'events',
  database: 'product',
})

const mockedRdsClient: jest.Mocked<RDSDataService> = store.client as jest.Mocked<
  RDSDataService
>

interface UserData {
  name: string
  age: number | null
}

type UserKey = string

interface UserMeta {
  createdAt: string
}

interface User {
  data: UserData
  meta: UserMeta
  key: UserKey
}

interface UserQuery extends Query<User> {
  filter?: {
    name?: EqualFilter<string> & PrefixFilter
    age?: OrdFilter<number>
  }
}

const user: User = {
  data: {
    name: 'Trevor',
    age: 47,
  },
  meta: { createdAt: 'yesterday' },
  key: 'u1',
}

test('AuroraPostgresStore#write', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.write(user)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    resourceArn: 'cluster-123',
    secretArn: 'secret-123',
    database: 'product',
    sql: `
  INSERT INTO events (id, item)
  VALUES(:id, :item::jsonb)
  ON CONFLICT (id) DO UPDATE SET item = :item::jsonb
`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user),
        },
      },
    ],
  })
})

test('AuroraPostgresStore#read', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: JSON.stringify(user) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.read('u1')).toEqual(user)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    resourceArn: 'cluster-123',
    secretArn: 'secret-123',
    database: 'product',
    sql: `SELECT item FROM events WHERE id = :id`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
    ],
  })

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  expect(await store.read('dne')).toBeUndefined()
})
