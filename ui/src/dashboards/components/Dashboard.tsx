// Libraries
import React, {PureComponent} from 'react'

// Components
import Cells from 'src/shared/components/cells/Cells'
import DashboardEmpty from 'src/dashboards/components/dashboard_empty/DashboardEmpty'
import {Page} from '@influxdata/clockface'

// Types
import {Cell} from 'src/types'
import {TimeRange} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cells: Cell[]
  timeRange: TimeRange
  manualRefresh: number
  onCloneCell: (cell: Cell) => void
  onPositionChange: (cells: Cell[]) => void
  onEditView: (cellID: string) => void
  onAddCell: () => void
  onEditNote: () => void
}

@ErrorHandling
class DashboardComponent extends PureComponent<Props> {
  public render() {
    const {
      cells,
      timeRange,
      manualRefresh,
      onCloneCell,
      onEditView,
      onPositionChange,
      onAddCell,
      onEditNote,
    } = this.props

    return (
      <Page.Contents fullWidth={true} scrollable={true} className="dashboard">
        {!!cells.length ? (
          <Cells
            cells={cells}
            timeRange={timeRange}
            manualRefresh={manualRefresh}
            onCloneCell={onCloneCell}
            onPositionChange={onPositionChange}
            onEditView={onEditView}
            onEditNote={onEditNote}
          />
        ) : (
          <DashboardEmpty onAddCell={onAddCell} />
        )}
        {/* This element is used as a portal container for note tooltips in cell headers */}
        <div className="cell-header-note-tooltip-container" />
      </Page.Contents>
    )
  }
}

export default DashboardComponent
