import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { OrdFilter, EqualFilter, PrefixFilter, Query } from 'imes'
import { AuroraPostgresStore } from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'users',
  database: 'product',
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

const user1: User = {
  data: {
    name: 'Trevor',
    age: 47,
  },
  meta: { createdAt: 'yesterday' },
  key: 'u1',
}

const user2: User = {
  data: {
    name: 'Whatever',
    age: 15,
  },
  meta: { createdAt: 'today' },
  key: 'u2',
}

const user3: User = {
  data: {
    name: 'Eternal',
    age: null,
  },
  meta: { createdAt: 'now' },
  key: 'u3',
}

test('AuroraPostgresStore#create', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.create(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO users (id, item)
  VALUES(:id, :item::jsonb)
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
          stringValue: JSON.stringify(user1),
        },
      },
    ],
  })
})

test('AuroraPostgresStore#update', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.update(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  UPDATE users
  SET item = :item::jsonb
  WHERE id = :id
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
    sql: `SELECT item FROM users WHERE id = :id`,
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
    items: [user1, user2, user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users`,
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
    items: [user1, user2],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users LIMIT 3`,
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
    items: [user2, user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE id > :id LIMIT 3`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
    ],
  })
})
