// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Types
import {Form, Input, Button} from '@influxdata/clockface'
import {Panel, Grid} from 'src/clockface'

// Types
import {Columns, ComponentSize, ComponentStatus} from '@influxdata/clockface'
import {MeState} from 'src/types/v2'

interface StateProps {
  me: MeState
}

interface State {
  me: MeState
}

export class Settings extends PureComponent<StateProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      me: this.props.me,
    }
  }

  public render() {
    const {me} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Six}>
            <Panel>
              <Panel.Header title="About Me">
                <Button text="Edit About Me" />
              </Panel.Header>
              <Panel.Body>
                <Form>
                  <Form.Element label="Username">
                    <Input
                      value={me.name}
                      testID="nameInput"
                      titleText="Username"
                      size={ComponentSize.Small}
                      status={ComponentStatus.Disabled}
                      onChange={this.handleChangeInput}
                    />
                  </Form.Element>
                </Form>
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleChangeInput = (_: ChangeEvent<HTMLInputElement>): void => {
    //  console.log('changing: ', e)
  }
}

const mstp = ({me}) => ({
  me,
})

export default connect<StateProps>(mstp)(Settings)
