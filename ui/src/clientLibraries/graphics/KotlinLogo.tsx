// Libraries
import React, {SFC} from 'react'

const KotlinLogo: SFC = () => {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 60 60">
      <linearGradient
        id="kotlin-a"
        gradientUnits="userSpaceOnUse"
        x1="15.959"
        y1="-13.014"
        x2="44.307"
        y2="15.333"
        gradientTransform="matrix(1 0 0 -1 0 61)"
      >
        <stop offset=".097" stop-color="#0095d5" />
        <stop offset=".301" stop-color="#238ad9" />
        <stop offset=".621" stop-color="#557bde" />
        <stop offset=".864" stop-color="#7472e2" />
        <stop offset="1" stop-color="#806ee3" />
      </linearGradient>
      <path fill="url(#kotlin-a)" d="M0 60l30.1-30.1L60 60z" />
      <linearGradient
        id="kotlin-b"
        gradientUnits="userSpaceOnUse"
        x1="4.209"
        y1="48.941"
        x2="20.673"
        y2="65.405"
        gradientTransform="matrix(1 0 0 -1 0 61)"
      >
        <stop offset=".118" stop-color="#0095d5" />
        <stop offset=".418" stop-color="#3c83dc" />
        <stop offset=".696" stop-color="#6d74e1" />
        <stop offset=".833" stop-color="#806ee3" />
      </linearGradient>
      <path fill="url(#kotlin-b)" d="M0 0h30.1L0 32.5z" />
      <linearGradient
        id="c"
        gradientUnits="userSpaceOnUse"
        x1="-10.102"
        y1="5.836"
        x2="45.731"
        y2="61.669"
        gradientTransform="matrix(1 0 0 -1 0 61)"
      >
        <stop offset=".107" stop-color="#c757bc" />
        <stop offset=".214" stop-color="#d0609a" />
        <stop offset=".425" stop-color="#e1725c" />
        <stop offset=".605" stop-color="#ee7e2f" />
        <stop offset=".743" stop-color="#f58613" />
        <stop offset=".823" stop-color="#f88909" />
      </linearGradient>
      <path fill="url(#c)" d="M30.1 0L0 31.7V60l30.1-30.1L60 0z" />
    </svg>
  )
}

export default KotlinLogo
