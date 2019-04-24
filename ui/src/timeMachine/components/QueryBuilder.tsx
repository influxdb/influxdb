// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {range} from 'lodash'

// Components
import TagSelector from 'src/timeMachine/components/TagSelector'
import QueryBuilderDataCard from 'src/timeMachine/components/QueryBuilderDataCard'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import FunctionSelector from 'src/timeMachine/components/FunctionSelector'
import AddCardButton from 'src/timeMachine/components/AddCardButton'

// Actions
import {loadBuckets, addTagSelector} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface StateProps {
  tagFiltersLength: number
  moreTags: boolean
}

interface DispatchProps {
  onLoadBuckets: () => Promise<void>
  onAddTagSelector: () => void
}

type Props = StateProps & DispatchProps

interface State {}

class TimeMachineQueryBuilder extends PureComponent<Props, State> {
  public componentDidMount() {
    this.props.onLoadBuckets()
  }

  public render() {
    const {tagFiltersLength} = this.props

    return (
      <div className="query-builder" data-testid="query-builder">
        <div className="query-builder--cards">
          <FancyScrollbar>
            <div className="builder-card--list">
              <QueryBuilderDataCard />
              {range(tagFiltersLength).map(i => (
                <TagSelector key={i} index={i} />
              ))}
              {this.addButton}
            </div>
          </FancyScrollbar>
          <FunctionSelector />
        </div>
      </div>
    )
  }

  private get addButton(): JSX.Element {
    const {moreTags, onAddTagSelector} = this.props

    if (!moreTags) {
      return null
    }

    return <AddCardButton onClick={onAddTagSelector} collapsible={false} />
  }
}

const mstp = (state: AppState): StateProps => {
  const tagFiltersLength = getActiveQuery(state).builderConfig.tags.length
  const tags = getActiveTimeMachine(state).queryBuilder.tags

  const {keys, keysStatus} = tags[tags.length - 1]

  return {
    tagFiltersLength,
    moreTags: !(keys.length === 0 && keysStatus === RemoteDataState.Done),
  }
}

const mdtp = {
  onLoadBuckets: loadBuckets as any,
  onAddTagSelector: addTagSelector,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueryBuilder)
