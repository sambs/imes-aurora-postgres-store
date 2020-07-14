import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { EqualFilter, EnumFilter, Query } from 'imes'
import { AuroraPostgresStore, AuroraPostgresStringIndex } from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

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

export interface UserQuery extends Query<User> {
  filter?: {
    name?: EqualFilter<string> & EnumFilter<string>
  }
}

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'users',
  database: 'product',
  indexes: {
    name: new AuroraPostgresStringIndex((user: User) => user.data.name),
  },
})

const mockedRdsClient: jest.Mocked<RDSDataService> = store.client as jest.Mocked<
  RDSDataService
>

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
  INSERT INTO users (id, item, name)
  VALUES(:id, :item::jsonb, :name)
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
      {
        name: 'name',
        value: {
          stringValue: 'Trevor',
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
  SET item = :item::jsonb, name = :name
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
      {
        name: 'name',
        value: {
          stringValue: 'Trevor',
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

test('AuroraPostgresStore#find with filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { name: { eq: 'Eternal' } } })).toEqual({
    cursor: null,
    items: [user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE name = :name__eq ORDER BY id`,
    parameters: [
      {
        name: 'name__eq',
        value: {
          stringValue: 'Eternal',
        },
      },
    ],
  })
})
