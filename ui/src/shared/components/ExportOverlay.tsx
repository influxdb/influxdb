import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {
  Form,
  Button,
  SpinnerContainer,
  TechnoSpinner,
  Overlay,
} from '@influxdata/clockface'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'
import CopyButton from 'src/shared/components/CopyButton'

// Actions
import {createTemplateFromResource} from 'src/templates/actions/'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {ComponentColor, ComponentSize} from '@influxdata/clockface'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  onDismissOverlay: () => void
  resource: DocumentCreate
  resourceName: string
  status: RemoteDataState
  isVisible: boolean
}

interface DispatchProps {
  onCreateTemplateFromResource: typeof createTemplateFromResource
}

type Props = OwnProps & DispatchProps

class ExportOverlay extends PureComponent<Props> {
  public static defaultProps = {
    isVisible: true,
  }

  public render() {
    const {isVisible, resourceName, onDismissOverlay, status} = this.props

    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800}>
          <Form onSubmit={this.handleExport}>
            <Overlay.Header
              title={`Export ${resourceName}`}
              onDismiss={onDismissOverlay}
            />
            <Overlay.Body>
              <SpinnerContainer
                loading={status}
                spinnerComponent={<TechnoSpinner />}
              >
                {this.overlayBody}
              </SpinnerContainer>
            </Overlay.Body>
            <Overlay.Footer>
              {this.downloadButton}
              {this.toTemplateButton}
              {this.copyButton}
            </Overlay.Footer>
          </Form>
        </Overlay.Container>
      </Overlay>
    )
  }

  private doNothing = () => {}

  private get overlayBody(): JSX.Element {
    const options = {
      tabIndex: 1,
      mode: 'json',
      readonly: true,
      lineNumbers: true,
      autoRefresh: true,
      theme: 'time-machine',
      completeSingle: false,
    }
    return (
      <div className="export-overlay--text-area">
        <ReactCodeMirror
          autoFocus={false}
          autoCursor={true}
          value={this.resourceText}
          options={options}
          onBeforeChange={this.doNothing}
          onTouchStart={this.doNothing}
        />
      </div>
    )
  }

  private get resourceText(): string {
    return JSON.stringify(this.props.resource, null, 1)
  }

  private get copyButton(): JSX.Element {
    return (
      <CopyButton
        textToCopy={this.resourceText}
        contentName={this.props.resourceName}
        size={ComponentSize.Small}
        color={ComponentColor.Secondary}
      />
    )
  }

  private get downloadButton(): JSX.Element {
    return (
      <Button
        text="Download JSON"
        onClick={this.handleExport}
        color={ComponentColor.Primary}
      />
    )
  }

  private get toTemplateButton(): JSX.Element {
    return (
      <Button
        text="Save as template"
        onClick={this.handleConvertToTemplate}
        color={ComponentColor.Primary}
      />
    )
  }

  private handleExport = (): void => {
    const {resource, resourceName, onDismissOverlay} = this.props
    const name = get(resource, 'name', resourceName)
    downloadTextFile(JSON.stringify(resource, null, 1), `${name}.json`)
    onDismissOverlay()
  }

  private handleConvertToTemplate = async (): Promise<void> => {
    const {
      resource,
      onDismissOverlay,
      resourceName,
      onCreateTemplateFromResource,
    } = this.props

    onCreateTemplateFromResource(resource, resourceName)
    onDismissOverlay()
  }
}

const mdtp: DispatchProps = {
  onCreateTemplateFromResource: createTemplateFromResource,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(ExportOverlay)
