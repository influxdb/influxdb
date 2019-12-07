// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {getFormatter} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, HeatmapViewProperties, TimeZone} from 'src/types'

interface Props {
  endTime: number
  loading: RemoteDataState
  startTime: number
  table: Table
  timeZone: TimeZone
  viewProperties: HeatmapViewProperties
  children: (config: Config) => JSX.Element
}

const HeatmapPlot: FunctionComponent<Props> = ({
  endTime,
  loading,
  startTime,
  table,
  timeZone,
  viewProperties: {
    xColumn,
    yColumn,
    xDomain: storedXDomain,
    yDomain: storedYDomain,
    xAxisLabel,
    yAxisLabel,
    xPrefix,
    xSuffix,
    yPrefix,
    ySuffix,
    colors: storedColors,
    binSize,
    timeFormat,
  },
  children,
}) => {
  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number'),
    startTime,
    endTime
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    table.getColumn(yColumn, 'number')
  )

  const isValidView =
    xColumn &&
    yColumn &&
    columnKeys.includes(yColumn) &&
    columnKeys.includes(xColumn)

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colors: string[] =
    storedColors && storedColors.length
      ? storedColors
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(table.getColumnType(xColumn), {
    prefix: xPrefix,
    suffix: xSuffix,
    timeZone,
    timeFormat,
  })

  const yFormatter = getFormatter(table.getColumnType(yColumn), {
    prefix: yPrefix,
    suffix: ySuffix,
    timeZone,
    timeFormat,
  })

  const config: Config = {
    ...VIS_THEME,
    table,
    xAxisLabel,
    yAxisLabel,
    xDomain,
    onSetXDomain,
    onResetXDomain,
    yDomain,
    onSetYDomain,
    onResetYDomain,
    valueFormatters: {
      [xColumn]: xFormatter,
      [yColumn]: yFormatter,
    },
    layers: [
      {
        type: 'heatmap',
        x: xColumn,
        y: yColumn,
        colors,
        binSize,
      },
    ],
  }

  return (
    <>
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </>
  )
}

export default HeatmapPlot
