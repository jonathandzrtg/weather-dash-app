import dash
from dash import dcc, html, dash_table, Input, Output, State
import os
import pandas as pd
from databricks.sql import connect

# Variables de conexión (definidas como variables de entorno)
DATBRICKS_API_KEY = os.getenv("DATABRICKS_API_KEY")
DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Nombre de la tabla en el Unity Catalog
TABLE_NAME = "brz_dev.dbdemos.colombian_temperature_data"

def execute_query(query: str) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL utilizando Databricks SQL Connector.
    Si la consulta es SELECT, devuelve un DataFrame.
    Para INSERT, UPDATE o DELETE, solo ejecuta la operación.
    """
    with connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATBRICKS_API_KEY,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            if query.strip().upper().startswith("SELECT"):
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                return pd.DataFrame(rows, columns=column_names)
            else:
                return None

# Inicializa la app de Dash
app = dash.Dash(__name__)
server = app.server

# Layout: Usamos Tabs para separar las operaciones CRUD
app.layout = html.Div([
    html.H1("CRUD App para Colombian Temperature Data", style={'textAlign': 'center'}),
    dcc.Tabs(id="tabs", value="read", children=[
        dcc.Tab(label="Read", value="read"),
        dcc.Tab(label="Create", value="create"),
        dcc.Tab(label="Update", value="update"),
        dcc.Tab(label="Delete", value="delete"),
    ]),
    html.Div(id="tab-content", style={'padding': '20px'})
])

# Callback para renderizar el contenido según la pestaña seleccionada
@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value")
)
def render_tab(tab):
    if tab == "read":
        return html.Div([
            html.H2("Visualizar Datos"),
            html.Button("Refrescar Datos", id="refresh-button"),
            dash_table.DataTable(
                id="data-table",
                columns=[{"name": i, "id": i} for i in [
                    "date", "codigoestacion", "municipio", "departamento",
                    "latitud", "longitud", "temp_min", "temp_avg", "temp_max"
                ]],
                data=[],
                page_size=10,
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'}
            )
        ])
    elif tab == "create":
        return html.Div([
            html.H2("Crear Nuevo Registro"),
            html.Label("Fecha (YYYY-MM-DD):"),
            dcc.Input(id="create-date", type="text", placeholder="Ej: 2022-01-15"),
            html.Br(),
            html.Label("Código Estación:"),
            dcc.Input(id="create-codigo", type="text"),
            html.Br(),
            html.Label("Municipio:"),
            dcc.Input(id="create-municipio", type="text"),
            html.Br(),
            html.Label("Departamento:"),
            dcc.Input(id="create-departamento", type="text"),
            html.Br(),
            html.Label("Latitud:"),
            dcc.Input(id="create-latitud", type="number"),
            html.Br(),
            html.Label("Longitud:"),
            dcc.Input(id="create-longitud", type="number"),
            html.Br(),
            html.Label("Temperatura Mínima:"),
            dcc.Input(id="create-temp_min", type="number"),
            html.Br(),
            html.Label("Temperatura Promedio:"),
            dcc.Input(id="create-temp_avg", type="number"),
            html.Br(),
            html.Label("Temperatura Máxima:"),
            dcc.Input(id="create-temp_max", type="number"),
            html.Br(),
            html.Button("Crear Registro", id="create-button"),
            html.Div(id="create-output", style={'marginTop': '10px', 'color': 'green'})
        ])
    elif tab == "update":
        return html.Div([
            html.H2("Actualizar Registro"),
            html.Label("Código Estación (identificador):"),
            dcc.Input(id="update-codigo", type="text"),
            html.Br(),
            html.Label("Campo a Actualizar (ej: temp_min, temp_avg, temp_max):"),
            dcc.Input(id="update-field", type="text"),
            html.Br(),
            html.Label("Nuevo Valor:"),
            dcc.Input(id="update-new_value", type="text"),
            html.Br(),
            html.Button("Actualizar Registro", id="update-button"),
            html.Div(id="update-output", style={'marginTop': '10px', 'color': 'green'})
        ])
    elif tab == "delete":
        return html.Div([
            html.H2("Eliminar Registro"),
            html.Label("Código Estación (identificador):"),
            dcc.Input(id="delete-codigo", type="text"),
            html.Br(),
            html.Button("Eliminar Registro", id="delete-button"),
            html.Div(id="delete-output", style={'marginTop': '10px', 'color': 'red'})
        ])

# Callback para la pestaña Read: refrescar y mostrar datos
@app.callback(
    Output("data-table", "data"),
    Input("refresh-button", "n_clicks"),
    prevent_initial_call=True
)
def refresh_data(n_clicks):
    query = f"SELECT * FROM {TABLE_NAME}"
    df = execute_query(query)
    if df is not None:
        return df.to_dict("records")
    return []

# Callback para la operación Create
@app.callback(
    Output("create-output", "children"),
    Input("create-button", "n_clicks"),
    State("create-date", "value"),
    State("create-codigo", "value"),
    State("create-municipio", "value"),
    State("create-departamento", "value"),
    State("create-latitud", "value"),
    State("create-longitud", "value"),
    State("create-temp_min", "value"),
    State("create-temp_avg", "value"),
    State("create-temp_max", "value"),
    prevent_initial_call=True
)
def create_record(n_clicks, date, codigo, municipio, departamento, latitud, longitud, temp_min, temp_avg, temp_max):
    query = f"""
    INSERT INTO {TABLE_NAME} 
    VALUES ('{date}', '{codigo}', '{municipio}', '{departamento}', {latitud}, {longitud}, {temp_min}, {temp_avg}, {temp_max})
    """
    try:
        execute_query(query)
        return "Registro creado exitosamente."
    except Exception as e:
        return f"Error al crear el registro: {str(e)}"

# Callback para la operación Update
@app.callback(
    Output("update-output", "children"),
    Input("update-button", "n_clicks"),
    State("update-codigo", "value"),
    State("update-field", "value"),
    State("update-new_value", "value"),
    prevent_initial_call=True
)
def update_record(n_clicks, codigo, field, new_value):
    # Se asume que los campos de texto requieren comillas
    if field in ["date", "codigoestacion", "municipio", "departamento"]:
        value_expr = f"'{new_value}'"
    else:
        value_expr = new_value
    query = f"""
    UPDATE {TABLE_NAME}
    SET {field} = {value_expr}
    WHERE codigoestacion = '{codigo}'
    """
    try:
        execute_query(query)
        return "Registro actualizado exitosamente."
    except Exception as e:
        return f"Error al actualizar el registro: {str(e)}"

# Callback para la operación Delete
@app.callback(
    Output("delete-output", "children"),
    Input("delete-button", "n_clicks"),
    State("delete-codigo", "value"),
    prevent_initial_call=True
)
def delete_record(n_clicks, codigo):
    query = f"""
    DELETE FROM {TABLE_NAME}
    WHERE codigoestacion = '{codigo}'
    """
    try:
        execute_query(query)
        return "Registro eliminado exitosamente."
    except Exception as e:
        return f"Error al eliminar el registro: {str(e)}"

if __name__ == '__main__':
    app.run_server(debug=True, port=5000)

