// Libraries
import React, {FC, useReducer} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import RuleSchedule from 'src/alerting/components/notifications/RuleSchedule'
import RuleConditions from 'src/alerting/components/notifications/RuleConditions'
import RuleMessage from 'src/alerting/components/notifications/RuleMessage'
import {
  Panel,
  ComponentSize,
  Overlay,
  Form,
  Input,
  Grid,
  Columns,
} from '@influxdata/clockface'

// Reducers
import {reducer} from './RuleOverlay.reducer'

// Constants
import {newRule, endpoints} from 'src/alerting/constants'

// Types
import {NotificationRuleDraft} from 'src/types'

// Hooks
import {RuleMode, NewRuleDispatch} from 'src/shared/hooks'

type Props = WithRouterProps

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const [rule, dispatch] = useReducer(reducer, newRule)

  const handleChange = e => {
    const {name, value} = e.target
    dispatch({
      type: 'UPDATE_RULE',
      rule: {...rule, [name]: value} as NotificationRuleDraft,
    })
  }

  return (
    <RuleMode.Provider value="NewRuleDispatch">
      <NewRuleDispatch.Provider value={dispatch}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={800}>
            <Overlay.Header
              title="Create a Notification Rule"
              onDismiss={handleDismiss}
            />
            <Overlay.Body>
              <Grid>
                <Form>
                  <Grid.Row>
                    <Grid.Column widthSM={Columns.Two}>About</Grid.Column>
                    <Grid.Column widthSM={Columns.Ten}>
                      <Panel size={ComponentSize.ExtraSmall}>
                        <Panel.Body>
                          <Form.Element label="Name">
                            <Input
                              testID="rule-name--input"
                              placeholder="Name this new rule"
                              value={rule.name}
                              name="name"
                              onChange={handleChange}
                            />
                          </Form.Element>
                          <RuleSchedule rule={rule} onChange={handleChange} />
                        </Panel.Body>
                      </Panel>
                    </Grid.Column>
                    <Grid.Column>
                      <hr />
                    </Grid.Column>
                  </Grid.Row>
                  <RuleConditions rule={rule} />
                  <RuleMessage rule={rule} endpoints={endpoints} />
                </Form>
              </Grid>
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </NewRuleDispatch.Provider>
    </RuleMode.Provider>
  )
}

export default withRouter<Props>(NewRuleOverlay)
