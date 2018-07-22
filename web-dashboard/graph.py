import base64
import datetime
import io

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

from random import *
import dash_table_experiments as dt

import pandas as pd

import plotly
import plotly.graph_objs as go
import json

app = dash.Dash()

app.scripts.config.serve_locally = True

list_of_states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
                  "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
                  "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
                  "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
                  "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
                  "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
                  "West Virginia", "Wisconsin", "Wyoming"]

list_of_short_codes = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
                       "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
                       "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
                       "WI", "WY"]

scl = [[0.0, '#FF0000'],[0.2, '#FF5700'],[0.4, '#FF9E00'], [0.5, '#F7FF00'],
        [0.6, '#D4FF00'], [0.8, '#4AB703'], [1.0, '#03B712']]


def get_states_data():
    dict_array = []
    for i in range(0, len(list_of_states), 1):
        dictionary = {'state': list_of_states[i], 'code': list_of_short_codes[i], 'sentiment': uniform(0, 1), 'weather': randint(-10, 40)}
        dictionary["text"] = dictionary["state"] + "<br>temperature : " + str(dictionary["weather"]) + " degree"
        dict_array.append(dictionary)
    return pd.DataFrame(dict_array)



def create_scatter_trace(df):
    data = [dict(
        type='choropleth',
        colorscale=scl,
        autocolorscale=False,
        locations=df['code'],
        z=df['sentiment'].astype(float),
        locationmode='USA-states',
        text=df['text'],
        marker=dict(
            line=dict(
                color='rgb(255,255,255)',
                width=2
            )),
        colorbar=dict(
            title="Sentiment Scale")
    )]

    layout = dict(
        title='Geo Spatial(US states) analysis',
        geo=dict(
            scope='usa',
            projection=dict(type='albers usa'),
            showlakes=True,
            lakecolor='rgb(255, 255, 255)'),
    )
    return go.Figure(data=data, layout=layout)


app.layout = html.Div([
    dcc.Input(
        placeholder='Enter search term...',
        type='text',
        value=''
    ),
    html.Button('Search', id='button'),
    html.Div([
        html.Div(
            dcc.Graph(
                id='graph',
                style={
                    'overflow-x': 'wordwrap'
                },
                figure=create_scatter_trace(get_states_data())
            )
        )
    ]),
    dcc.Interval(
        id='interval-component',
        interval=1*2000, # in milliseconds
        n_intervals=0
    )
])


@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    return create_scatter_trace(get_states_data())


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)
