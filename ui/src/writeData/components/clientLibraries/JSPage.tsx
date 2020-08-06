// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryPage from 'src/writeData/components/clientLibraries/ClientLibraryPage'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientJSLibrary} from 'src/clientLibraries/constants'

// Types
import {AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientJSPage: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    initializeNPMCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
  } = clientJSLibrary
  const {org} = props
  const server = window.location.origin

  return (
    <ClientLibraryPage title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <h5>Install via NPM</h5>
      <TemplatedCodeSnippet template={initializeNPMCodeSnippet} label="Code" />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="JavaScript Code"
        defaults={{
          server: 'basepath',
          token: 'token',
          org: 'orgID',
          bucket: 'bucketID',
        }}
        values={{
          server,
          org,
        }}
      />
      <h5>Write Data</h5>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="JavaScript Code"
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="JavaScript Code"
      />
    </ClientLibraryPage>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)

  return {
    org: id,
  }
}

export {ClientJSPage}
export default connect<StateProps, {}, Props>(mstp)(ClientJSPage)
