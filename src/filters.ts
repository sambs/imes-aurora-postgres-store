import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

export const eqFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__eq`
  return {
    where: `${index} = :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const neFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__ne`
  return {
    where: `${index} <> :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const inFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (values: T[]) => {
  const items = values.map((value, valueIndex) => {
    const paramName = `${index}__in_${valueIndex}`
    return {
      paramName,
      parameter: {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    }
  })
  return {
    where: `${index} IN (:${items
      .map(({ paramName }) => paramName)
      .join(', :')})`,
    parameters: items.map(({ parameter }) => parameter),
  }
}

export const gtFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__gt`
  return {
    where: `${index} > :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const gteFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__gte`
  return {
    where: `${index} >= :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const ltFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__lt`
  return {
    where: `${index} < :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const lteFilter = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => (value: T) => {
  const paramName = `${index}__lte`
  return {
    where: `${index} <= :${paramName}`,
    parameters: [
      {
        name: paramName,
        value: auroraValue(value),
        typeHint,
      },
    ],
  }
}

export const exactFilters = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => ({
  eq: eqFilter(index, auroraValue, typeHint),
  ne: neFilter(index, auroraValue, typeHint),
  in: inFilter(index, auroraValue, typeHint),
})

export const ordFilters = <T>(
  index: string,
  auroraValue: (value: T) => RDSDataService.Field,
  typeHint?: RDSDataService.TypeHint
) => ({
  eq: eqFilter(index, auroraValue, typeHint),
  lt: ltFilter(index, auroraValue, typeHint),
  lte: lteFilter(index, auroraValue, typeHint),
  gt: gtFilter(index, auroraValue, typeHint),
  gte: gteFilter(index, auroraValue, typeHint),
})
