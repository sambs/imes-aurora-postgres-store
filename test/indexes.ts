import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { ExactFilter, OrdFilter, Query } from 'imes'

import {
  AuroraPostgresStore,
  auroraLongValue,
  auroraNullable,
  auroraStringValue,
  exactFilters,
  ordFilters,
} from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

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

interface UserQuery extends Query {
  filter?: {
    name?: ExactFilter<string>
    age?: OrdFilter<number>
  }
}

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'users',
  database: 'product',
  getItemKey: ({ id }) => id,
  indexes: {
    name: {
      pick: user => user.name,
      value: auroraStringValue,
    },
    age: {
      pick: user => user.age,
      value: auroraNullable(auroraLongValue),
    },
  },
  filters: {
    name: exactFilters('name', auroraStringValue),
    age: ordFilters('age', auroraLongValue),
  },
})

const mockedRdsClient: jest.Mocked<RDSDataService> = store.client as jest.Mocked<
  RDSDataService
>

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

test('AuroraPostgresStore#create', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.create(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO users (key, item, name, age)
  VALUES(:key, :item::jsonb, :name, :age)
`,
    parameters: [
      {
        name: 'key',
        value: { stringValue: 'u1' },
      },
      {
        name: 'item',
        value: { stringValue: JSON.stringify(user1) },
      },
      {
        name: 'name',
        value: { stringValue: 'Trevor' },
        typeHint: undefined,
      },
      {
        name: 'age',
        value: { longValue: 47 },
        typeHint: undefined,
      },
    ],
  })
})

test('AuroraPostgresStore#create with a null value', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.create(user3)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO users (key, item, name, age)
  VALUES(:key, :item::jsonb, :name, :age)
`,
    parameters: [
      {
        name: 'key',
        value: {
          stringValue: 'u3',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user3),
        },
      },
      {
        name: 'name',
        value: { stringValue: 'Eternal' },
        typeHint: undefined,
      },
      {
        name: 'age',
        value: { isNull: true },
        typeHint: undefined,
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
  SET item = :item::jsonb, name = :name, age = :age
  WHERE key = :key
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
      {
        name: 'name',
        value: {
          stringValue: 'Trevor',
        },
      },
      {
        name: 'age',
        value: {
          longValue: 47,
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
    sql: `SELECT item FROM users WHERE key = :key`,
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

test('AuroraPostgresStore#find with eq filter', async () => {
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
    sql: `SELECT key,item FROM users WHERE name = :name__eq ORDER BY key`,
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

test('AuroraPostgresStore#find with ne filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }],
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { name: { ne: 'Eternal' } } })).toEqual({
    cursor: null,
    items: [user1, user2],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM users WHERE name <> :name__ne ORDER BY key`,
    parameters: [
      {
        name: 'name__ne',
        value: {
          stringValue: 'Eternal',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#find with in filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(
    await store.find({ filter: { name: { in: ['Eternal', 'Sunshine'] } } })
  ).toEqual({
    cursor: null,
    items: [user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM users WHERE name IN (:name__in_0, :name__in_1) ORDER BY key`,
    parameters: [
      {
        name: 'name__in_0',
        value: {
          stringValue: 'Eternal',
        },
      },
      {
        name: 'name__in_1',
        value: {
          stringValue: 'Sunshine',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#find with a gt filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { age: { gt: 45 } } })).toEqual({
    cursor: null,
    items: [user1],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT key,item FROM users WHERE age > :age__gt ORDER BY key`,
    parameters: [
      {
        name: 'age__gt',
        value: {
          longValue: 45,
        },
      },
    ],
  })
})
