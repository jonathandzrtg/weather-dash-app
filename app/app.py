import dash
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import os
import pandas as pd
from databricks.sql import connect


DATBRICKS_API_KEY = os.getenv("DATABRICKS_API_KEY")
DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

query = "SELECT * FROM brz_dev.dbdemos.colombian_temperature_data"


def query_to_dataframe(query: str) -> pd.DataFrame:
    with connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATBRICKS_API_KEY,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
 
    return pd.DataFrame(rows, columns=column_names)

# Load your data

df = query_to_dataframe(query)
df['date'] = pd.to_datetime(df['date'])  # Ensure 'date' column is datetime type
df["departamento"]=df["departamento"].str.strip()
df["departamento"]=df["departamento"].str.replace("SAN ANDRES Y  PROVIDENCIA","SAN ANDRES Y PROVIDENCIA")
df["municipio"]=df["municipio"].str.strip()


def select_municipio(df,municipio):
    df=df[df["municipio"]==municipio].\
    groupby(["date"])[["temp_min","temp_avg","temp_max"]].mean().reset_index().sort_values(by="date",ascending=False).reset_index(drop=True)
    return df

def monthly_evolution_of_temperature_per_municipio(df, departamento, municipio, min_date, max_date, variable):
    labels_vars = {
        "temp_max": "Temperatura máxima",
        "temp_avg": "Temperatura promedio",
        "temp_min": "Temperatura mínima",
        "precipitacion_total": "Precipitación total"
    }

    df_temp = select_municipio(df, municipio)

    df_temp.set_index('date', inplace=True)

    # Resample data to get mean values per month and round to 1 decimal
    df_temp = df_temp.resample('M').mean().round(1)

    # Reset index to have 'date' as a regular column
    df_temp.reset_index(inplace=True)

    df_temp = df_temp[(df_temp[variable] >= 0) & (df_temp[variable] <= 45) & (df_temp["date"] >= pd.to_datetime(min_date)) & (df_temp["date"] <= pd.to_datetime(max_date))]
    df_temp['id'] = range(len(df_temp))

     # Check if df_temp is empty
    if df_temp.empty:
        max_dates = max_date  # Use the user-specified max_date as a fallback
    else:
        max_dates = df_temp["date"].max().strftime("%Y-%m-%d")

    frames = []
    for i in range(len(df_temp)):
        frame = df_temp[df_temp['id'] <= i].copy()
        frame['frame'] = i
        frames.append(frame)
        
    # Check if frames is not empty before concatenation
    if frames:
        animated_df = pd.concat(frames)
    else:
        # Handling the case where frames is empty
        # Create an empty DataFrame with necessary columns for plotly to avoid error
        animated_df = pd.DataFrame(columns=['date', variable, 'id', 'frame'])


    if animated_df.empty:
        fig = px.scatter(title=f'Sin datos disponibles para {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}')
    else:
        fig = px.scatter(animated_df, x='date', y=variable, 
                         title=f'Evolución de promedio de {labels_vars[variable]}(°C) Mensual de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}',
                         labels= {variable: "°C", 'date': 'Fecha'},
                         animation_frame='frame',
                         size=variable,
                         size_max=20,
                         color=variable,
                         color_continuous_scale=px.colors.diverging.Portland,
                         opacity=0.7)

        # Removed explicit range settings for autoscaling

        # Set animation duration
        fig.update_layout(
            updatemenus=[dict(type='buttons', 
                                showactive=False, 
                                buttons=[dict(label='Play',
                                            method='animate',
                                            args=[None, dict(frame=dict(duration=50, redraw=True), fromcurrent=True)]),
                                        dict(label='Pause',
                                            method='animate',
                                            args=[[None], dict(frame=dict(duration=0, redraw=True), mode='immediate', transition=dict(duration=0))])])],
            #height=500,  # Set the height of the plot here. Adjust the value as needed.
            title=dict(text=f'Evolución de promedio de {labels_vars[variable]}(°C) Mensual de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}',
                       x=0.5,  # Center the title
                       xanchor='center',  # Use 'center' to center
                       font=dict(size=20)  # Adjust the font size here
                      )
                                                      )
        fig.update_xaxes(type='date', tickformat='%Y-%m', title_text='Fecha', tickangle=45)
        fig.update_xaxes(tickmode='linear', tick0=animated_df['date'].min(), dtick='M3')

        #fig.show()
    return fig

def weekly_evolution_of_temperature_per_municipio(df, departamento, municipio, min_date, max_date, variable):
    labels_vars = {
        "temp_max": "Temperatura máxima",
        "temp_avg": "Temperatura promedio",
        "temp_min": "Temperatura mínima"
    }

    df_temp = select_municipio(df, municipio)

    df_temp.set_index('date', inplace=True)

    # Resample data to get mean values per week and round to 1 decimal
    df_temp = df_temp.resample('W-Mon').mean().round(1)

    # Reset index to have 'date' as a regular column
    df_temp.reset_index(inplace=True)

    df_temp = df_temp[(df_temp[variable] >= 0) & (df_temp[variable] <= 45) & (df_temp["date"] >= pd.to_datetime(min_date)) & (df_temp["date"] <= pd.to_datetime(max_date))]
    df_temp['id'] = range(len(df_temp))

    if df_temp.empty:
        max_dates = max_date  # Use the user-specified max_date as a fallback
    else:
        max_dates = df_temp["date"].max().strftime("%Y-%m-%d")

    frames = []
    for i in range(len(df_temp)):
        frame = df_temp[df_temp['id'] <= i].copy()
        frame['frame'] = i
        frames.append(frame)
        
    animated_df = pd.concat(frames) if frames else pd.DataFrame(columns=['date', variable, 'id', 'frame'])

    if animated_df.empty:
        fig = px.scatter(title=f'Sin datos disponibles para {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}')
    else:
        fig = px.scatter(animated_df, x='date', y=variable, 
                         title=f'Evolución de promedio de {labels_vars[variable]}(°C) Semanal de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}',
                         labels={variable: "°C", 'date': 'Fecha'},
                         animation_frame='frame',
                         size=variable,
                         size_max=20,
                         color=variable,
                         color_continuous_scale=px.colors.diverging.Portland,
                         opacity=0.7)

        fig.update_layout(
            updatemenus=[{
                'type': 'buttons', 
                'showactive': False, 
                'buttons': [
                    {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 50, 'redraw': True}, 'fromcurrent': True}]},
                    {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'transition': {'duration': 0}}]}
                ]
            }],
            #height=500,
            title={
                'text': f'Evolución de promedio de {labels_vars[variable]}(°C) Semanal de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} and {max_dates}',
                'x': 0.5, 'xanchor': 'center', 'font': {'size': 20}
            }
        )
        fig.update_xaxes(type='date', tickformat='%Y-%m-%d', title_text='Fecha', tickangle=45)
        fig.update_xaxes(tickmode='linear', tick0=animated_df['date'].min(), dtick='M3')  

    return fig

def quarterly_evolution_of_temperature_per_municipio(df, departamento, municipio, min_date, max_date, variable):
    labels_vars = {
        "temp_max": "Temperatura máxima",
        "temp_avg": "Temperatura promedio",
        "temp_min": "Temperatura mínima"
    }

    df_temp = select_municipio(df, municipio)

    df_temp.set_index('date', inplace=True)

    # Resample data to get mean values per quarter and round to 1 decimal
    df_temp = df_temp.resample('Q').mean().round(1)

    # Reset index to have 'date' as a regular column
    df_temp.reset_index(inplace=True)

    df_temp = df_temp[(df_temp[variable] >= 0) & (df_temp[variable] <= 45) & (df_temp["date"] >= pd.to_datetime(min_date)) & (df_temp["date"] <= pd.to_datetime(max_date))]
    df_temp['id'] = range(len(df_temp))

    if df_temp.empty:
        max_dates = max_date  # Use the user-specified max_date as a fallback
    else:
        max_dates = df_temp["date"].max().strftime("%Y-%m-%d")

    frames = []
    for i in range(len(df_temp)):
        frame = df_temp[df_temp['id'] <= i].copy()
        frame['frame'] = i
        frames.append(frame)
        
    animated_df = pd.concat(frames) if frames else pd.DataFrame(columns=['date', variable, 'id', 'frame'])

    if animated_df.empty:
        fig = px.scatter(title=f'Sin datos disponibles para {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}')
    else:
        fig = px.scatter(animated_df, x='date', y=variable, 
                         title=f'Evolución de promedio de {labels_vars[variable]}(°C) Trimestral de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}',
                         labels={variable: "°C", 'date': 'Fecha'},
                         animation_frame='frame',
                         size=variable,
                         size_max=20,
                         color=variable,
                         color_continuous_scale=px.colors.diverging.Portland,
                         opacity=0.7)

        fig.update_layout(
            updatemenus=[{
                'type': 'buttons', 
                'showactive': False, 
                'buttons': [
                    {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 50, 'redraw': True}, 'fromcurrent': True}]},
                    {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'transition': {'duration': 0}}]}
                ]
            }],
            #height=500,
            title={
                'text': f'Evolución de promedio de {labels_vars[variable]}(°C) Trimestral de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} and {max_dates}',
                'x': 0.5, 'xanchor': 'center', 'font': {'size': 20}
            }
        )
        fig.update_xaxes(type='date', tickformat='%Y-Q%q', title_text='Fecha', tickangle=45)
        fig.update_xaxes(tickmode='linear', tick0=animated_df['date'].min(), dtick='M3') 
    
    return fig

def yearly_evolution_of_temperature_per_municipio(df, departamento, municipio, min_date, max_date, variable):
    labels_vars = {
        "temp_max": "Temperatura máxima",
        "temp_avg": "Temperatura promedio",
        "temp_min": "Temperatura mínima"
    }

    df_temp = select_municipio(df, municipio)

    df_temp.set_index('date', inplace=True)

    # Resample data to get mean values per year and round to 1 decimal
    df_temp = df_temp.resample('A').mean().round(1)

    # Reset index to have 'date' as a regular column
    df_temp.reset_index(inplace=True)

    df_temp = df_temp[(df_temp[variable] >= 0) & (df_temp[variable] <= 45) & (df_temp["date"] >= pd.to_datetime(min_date)) & (df_temp["date"] <= pd.to_datetime(max_date))]
    df_temp['id'] = range(len(df_temp))

    if df_temp.empty:
        max_dates = max_date  # Use the user-specified max_date as a fallback
    else:
        max_dates = df_temp["date"].max().strftime("%Y-%m-%d")

    frames = []
    for i in range(len(df_temp)):
        frame = df_temp[df_temp['id'] <= i].copy()
        frame['frame'] = i
        frames.append(frame)
        
    animated_df = pd.concat(frames) if frames else pd.DataFrame(columns=['date', variable, 'id', 'frame'])

    if animated_df.empty:
        fig = px.scatter(title=f'Sin datos disponibles para {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}')
    else:
        fig = px.scatter(animated_df, x='date', y=variable, 
                         title=f'Evolución de promedio de {labels_vars[variable]}(°C) Anual de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} y {max_dates}',
                         labels={variable: "°C", 'date': 'Fecha'},
                         animation_frame='frame',
                         size=variable,
                         size_max=20,
                         color=variable,
                         color_continuous_scale=px.colors.diverging.Portland,
                         opacity=0.7)

        fig.update_layout(
            updatemenus=[{
                'type': 'buttons', 
                'showactive': False, 
                'buttons': [
                    {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 50, 'redraw': True}, 'fromcurrent': True}]},
                    {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'transition': {'duration': 0}}]}
                ]
            }],
            #height=500,
            title={
                'text': f'Evolución de promedio de {labels_vars[variable]}(°C) Anual de {municipio.capitalize()}, {departamento.capitalize()} entre {min_date} and {max_dates}',
                'x': 0.5, 'xanchor': 'center', 'font': {'size': 20}
            }
        )
        fig.update_xaxes(type='date', tickformat='%Y', title_text='Fecha', tickangle=45)

    return fig



app = dash.Dash(__name__)

server=app.server

# Define the app layout
app.layout = html.Div([
    html.H1("Evolución de temperatura en municipios de Colombia.",style={'marginLeft': '30px', 'textAlign': 'left'}),
    html.Div([
        html.P(["Explore la evolución de la temperatura en los municipios de Colombia. Desarrollado con datos del IDEAM por ",
        html.A("David Alejandro López Atehortúa." , href="https://www.linkedin.com/in/davidalopeza", target="_blank"),
        ],style={'marginLeft': '30px','marginRight': '30px', 'fontSize': '21px', 'textAlign': 'left'}),
    ]),

    html.Div([
        html.Div([
            html.Label("Rango de fechas:",style={'fontSize': '20px', 'marginLeft': '30px','font-weight': 'bold'}),
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=df['date'].min().date(),
                max_date_allowed=df['date'].max().date(),
                start_date='2017-01-01',
                end_date=df['date'].max().date(),
                display_format='YYYY-MM-DD',
                start_date_placeholder_text="YYYY-MM-DD",
                end_date_placeholder_text ="YYYY-MM-DD",
                style={'width': '290px', 'height': '10px','fontSize': '19px', 'marginLeft': '30px'},
                clearable=False
            ),
        ], style={'marginTop': '10px','marginLeft': '5', 'width': '290px', 'height': '10px','display': 'inline-block'}),

            # Aggregation level selection
        html.Div([
            html.Label("Nivel agrupación:", style={'marginLeft': '31px', 'fontSize': '20px','font-weight': 'bold'}),
            #dcc.RadioItems(
            dcc.Dropdown(
                id='aggregation-level',
                options=[
                    #{'label': 'Semanal', 'value': 'W'},
                    {'label': 'Mensual', 'value': 'M'},
                    {'label': 'Trimestral', 'value': 'Q'},
                    {'label': 'Anual', 'value': 'A'}
                ],
                clearable=False,
                #inline=False,
                value='M',  # Default selection
                #labelStyle={'display': 'inline-block', 'marginRight': 20},
                style={'fontSize': '19px', 'height': '30px','width': '155px','margin':'5px','marginLeft':'15px'}
            ),
        ],style={'marginTop': '10px','fontSize': '19px', 'height': '50px','width': '190px','marginDown':'5px','marginLeft':'15px'}),


        html.Div([
            html.Label("Departamento:", style={'fontSize': '20px','marginLeft':'12px','font-weight': 'bold'}),
            dcc.Dropdown(
                id='departamento-dropdown',
                options=[{'label': i, 'value': i} for i in df['departamento'].sort_values(ascending=True).unique()],
                value="ANTIOQUIA",  # Default value
                style={'width': '210px', 'height': '30px', 'marginLeft':'24px' ,'fontSize': '19px',"margin":"5px"},
                clearable=False
            ),
        ], style={'marginTop': '10px','marginLeft': '10px', 'width': '210px', 'display': 'inline-block'}),
        
        html.Div([
            html.Label("Municipio:", style={'marginLeft':'20px' ,'fontSize': '20px','font-weight': 'bold'}),
            dcc.Dropdown(
                id='municipio-dropdown',
                # Options will be updated based on departamento selection
                value="MEDELLÍN",
                style={'width': '210px', 'height': '30px', 'margin':'5px', 'fontSize': '19px','marginLeft':'10px'},
                clearable=False
            ),  
        ], style={'marginTop': '10px','marginLeft': '10px', 'width': '210px', 'display': 'inline-block'}),


        html.Div([
            html.Label("Variable de Temperatura:",style={'marginLeft': '39px','fontSize': '20px','font-weight': 'bold'},),
            dcc.Dropdown(
                id='variable-dropdown',
                options=[
                    {'label': 'Temperatura Máxima', 'value': 'temp_max'},
                    {'label': 'Temperatura Promedio', 'value': 'temp_avg'},
                    {'label': 'Temperatura Mínima', 'value': 'temp_min'}
                ],
                value='temp_max',  # Default value
                style={'width': '210px', 'height': '30px','margin': '5px','fontSize': '19px','marginLeft': '20px'},
                clearable=False
            ),
        ], style={'marginTop': '10px','marginDown': '10px','width': '210', 'display': 'inline-block','height': '65px'}),
    ], style={'display': 'flex', 'flexWrap': 'wrap','flexDirection': 'row'}),

    html.Div([
        dcc.Graph(
            id='temperature-evolution-graph',
            config={'responsive': True}
        )
    ], style={'height': 'auto'}),#, 'margin': 'auto'}),
    #dcc.Graph(id='temperature-evolution-graph',config={'responsive': True}),
    html.Div(id='dummy-div', style={'display': 'none'})

])

app.clientside_callback(
    """
    function(figure) {
        setTimeout(function() {
            const playButton = document.querySelector('.updatemenu-button');
            if (playButton) {
                playButton.dispatchEvent(new Event('click'));
            }
        }, 1000); // Adjust the timeout as needed
        return window.dash_clientside.no_update;
    }
    """,
    Output('dummy-div', 'children'),  # A dummy output, you might need to add an invisible Div for this purpose
    [Input('temperature-evolution-graph', 'figure')],  # Triggered when the figure updates
)


@app.callback(
    [Output('municipio-dropdown', 'options'),
     Output('municipio-dropdown', 'value')],
    [Input('departamento-dropdown', 'value')]
)
def set_cities_options(selected_departamento):
    dff = df[df['departamento'] == selected_departamento]
    municipio_options = [{'label': i, 'value': i} for i in dff['municipio'].sort_values(ascending=True).unique()]
    default_municipio = "MEDELLÍN" if "MEDELLÍN" in dff['municipio'].values else municipio_options[0]['value']
    return municipio_options, default_municipio


@app.callback(
    Output('temperature-evolution-graph', 'figure'),
    [
        Input('aggregation-level', 'value'),
        Input('departamento-dropdown', 'value'),
        Input('municipio-dropdown', 'value'),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('variable-dropdown', 'value')
    ]
)
def update_graph(aggregation_level,selected_departamento, selected_municipio, start_date, end_date, selected_variable):
    filtered_df = df[(df['departamento'] == selected_departamento) & (df['municipio'] == selected_municipio)]

    if aggregation_level == 'W':
        fig = weekly_evolution_of_temperature_per_municipio(filtered_df, selected_departamento, selected_municipio, start_date, end_date, selected_variable)
    elif aggregation_level == 'M':
        fig = monthly_evolution_of_temperature_per_municipio(filtered_df, selected_departamento, selected_municipio, start_date, end_date, selected_variable)
    elif aggregation_level == 'Q':
        fig = quarterly_evolution_of_temperature_per_municipio(filtered_df, selected_departamento, selected_municipio, start_date, end_date, selected_variable)
    elif aggregation_level == 'A':
        fig = yearly_evolution_of_temperature_per_municipio(filtered_df, selected_departamento, selected_municipio, start_date, end_date, selected_variable)
    else:
        # You can customize this part to show a more specific message or an empty plot
        fig.update_layout(title_text='Nivel de agrupación no reconocido o no seleccionado')

    return fig

if __name__ == '__main__':
    app.run_server(debug=False)
