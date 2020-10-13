import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'

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
