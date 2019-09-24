// Libraries
import React, {FC, useState, useRef} from 'react'

// Utils
import {Scale} from '@influxdata/giraffe'
import {isInDomain} from 'src/shared/utils/vis'

// Components
import BoxTooltip from './BoxTooltip'

//Types
import {StatusRow, LowercaseCheckStatusLevel} from 'src/types'
import EventMarkerTooltip from './EventMarkerTooltip'

interface Props {
  events: StatusRow[]
  xScale: Scale<number, number>
  xDomain: number[]
}

const findMaxLevel = (event: StatusRow[]) => {
  const levels: LowercaseCheckStatusLevel[] = ['crit', 'warn', 'info', 'ok']
  const eventLevels = event.map(e => e.level)
  for (let l of levels) {
    if (eventLevels.includes(l)) {
      return l
    }
  }
  return 'unknown'
}

const EventMarker: FC<Props> = ({xScale, xDomain, events}) => {
  const trigger = useRef<HTMLDivElement>(null)

  const [tooltipVisible, setTooltipVisible] = useState(false)
  let triggerRect: DOMRect = null

  if (trigger.current) {
    triggerRect = trigger.current.getBoundingClientRect() as DOMRect
  }

  const {time} = events[0]
  const level = findMaxLevel(events)

  const x = xScale(time)
  const style = {left: `${x}px`}
  const levelClass = `event-marker--${level.toLowerCase()}`

  return (
    isInDomain(time, xDomain) && (
      <>
        <div className={`event-marker--line ${levelClass}`} style={style} />
        <div
          className={`event-marker--line-rect ${levelClass}`}
          style={style}
          ref={trigger}
          onMouseEnter={() => {
            setTooltipVisible(true)
          }}
          onMouseLeave={() => {
            setTooltipVisible(false)
          }}
        />
        {tooltipVisible && (
          <BoxTooltip triggerRect={triggerRect as DOMRect}>
            <EventMarkerTooltip events={events} />
          </BoxTooltip>
        )}
      </>
    )
  )
}

export default EventMarker
