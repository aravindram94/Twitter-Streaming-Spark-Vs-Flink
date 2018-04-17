import base64
import datetime
import io

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt

import pandas as pd

import plotly
import plotly.graph_objs as go
import json

app = dash.Dash()

app.scripts.config.serve_locally = True

# data = [{'x': 100, 'y': 200}, {'x': 300, 'y': 400}];
# dff = pd.DataFrame(data)

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_us_ag_exports.csv')

for col in df.columns:
    df[col] = df[col].astype(str)

scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],\
            [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]

df['text'] = df['state'] + '<br>' +\
    'Beef '+df['beef']+' Dairy '+df['dairy']+'<br>'+\
    'Fruits '+df['total fruits']+' Veggies ' + df['total veggies']+'<br>'+\
    'Wheat '+df['wheat']+' Corn '+df['corn']


def create_scatter_trace():
    data = [dict(
        type='choropleth',
        colorscale=scl,
        autocolorscale=False,
        locations=df['code'],
        z=df['total exports'].astype(float),
        locationmode='USA-states',
        text=df['text'],
        marker=dict(
            line=dict(
                color='rgb(255,255,255)',
                width=2
            )),
        colorbar=dict(
            title="Millions USD")
    )]

    layout = dict(
        title='2011 US Agriculture Exports by State<br>(Hover for breakdown)',
        geo=dict(
            scope='usa',
            projection=dict(type='albers usa'),
            showlakes=True,
            lakecolor='rgb(255, 255, 255)'),
    )
    return go.Figure(data=data, layout=layout)

app.layout = html.Div([
    html.Div([
        html.Div(
            dcc.Graph(
                id='graph',
                style={
                    'overflow-x': 'wordwrap'
                },
                figure=create_scatter_trace()
            )
        )
    ])
])

# @app.callback(
#     Output('click-data', 'children'),
#     [Input('graph', 'clickData')])
# def display_click_data(clickData):
#     return json.dumps(clickData, indent=2)


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)
