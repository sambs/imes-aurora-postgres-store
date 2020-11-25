import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { AuroraPostgresStore } from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

const store = new AuroraPostgresStore<User, {}>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'users',
  database: 'product',
  getItemKey: ({ id }) => id,
  indexes: {},
  filters: {},
})

const mockedRdsClient: jest.Mocked<RDSDataService> = store.client as jest.Mocked<
  RDSDataService
>

const commonQueryParams = {
  resourceArn: 'cluster-123',
  secretArn: 'secret-123',
  database: 'product',
}

interface UserData {
  id: string
  name: string
  age: number | null
}

interface UserMeta {
  createdAt: string
}

type User = UserData & UserMeta

const user1: User = {
  age: 47,
  createdAt: 'yesterday',
  id: 'u1',
  name: 'Trevor',
}

const user2: User = {
  age: 15,
  createdAt: 'today',
  id: 'u2',
  name: 'Whatever',
}

const user3: User = {
  age: null,
  createdAt: 'now',
  id: 'u3',
  name: 'Eternal',
}

test('AuroraPostgresStore#put', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.put(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO "users" ("key", "item")
  VALUES(:key, :item::jsonb)
  ON CONFLICT (key)
  DO UPDATE SET item = Excluded.item
`,
    parameters: [
      {
        name: 'key',
        value: {
          stringValue: 'u1',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user1),
        },
      },
    ],
  })
})

test('AuroraPostgresStore#get', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: JSON.stringify(user1) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.get('u1')).toEqual(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT item FROM "users" WHERE key = :key`,
    parameters: [
      {
        name: 'key',
        value: {
          stringValue: 'u1',
        },
      },
    ],
  })

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  expect(await store.get('dne')).toBeUndefined()
})

test('AuroraPostgresStore#getMany', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
      [{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.getMany(['u1', 'u2', 'dne'])).toEqual([
    user1,
    user2,
    undefined,
  ])

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM "users" WHERE key in (:key0, :key1, :key2)`,
    parameters: [
      { name: 'key0', value: { stringValue: 'u1' } },
      { name: 'key1', value: { stringValue: 'u2' } },
      { name: 'key2', value: { stringValue: 'dne' } },
    ],
  })

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  expect(await store.get('dne')).toBeUndefined()
})

test('AuroraPostgresStore#find', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }],
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
      [{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({})).toEqual({
    cursor: null,
    edges: [
      { cursor: 'u1', node: user1 },
      { cursor: 'u2', node: user2 },
      { cursor: 'u3', node: user3 },
    ],
    items: [user1, user2, user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM "users" ORDER BY key`,
    parameters: [],
  })
})

test('AuroraPostgresStore#find with limit', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }],
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
      [{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ limit: 2 })).toEqual({
    cursor: 'u2',
    edges: [
      { cursor: 'u1', node: user1 },
      { cursor: 'u2', node: user2 },
    ],
    items: [user1, user2],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM "users" ORDER BY key LIMIT 3`,
    parameters: [],
  })
})

test('AuroraPostgresStore#find with limit and cursor', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
      [{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ cursor: 'u1', limit: 2 })).toEqual({
    cursor: null,
    edges: [
      { cursor: 'u2', node: user2 },
      { cursor: 'u3', node: user3 },
    ],
    items: [user2, user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM "users" WHERE key > :key ORDER BY key LIMIT 3`,
    parameters: [
      {
        name: 'key',
        value: {
          stringValue: 'u1',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#setup', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.setup()

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: 'CREATE TABLE "users" ("key" varchar(64) PRIMARY KEY, "item" jsonb)',
  })
})
