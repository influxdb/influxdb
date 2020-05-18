import {register} from 'src/notebooks'
import ExampleView from './view'
import './style.scss'

let counter = 0

register({
  type: 'example',
  component: ExampleView,
  button: 'Example Adding',
  empty: () => ({
    text: 'Example Text ' + counter++,
  }),
})
