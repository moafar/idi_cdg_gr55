import pandas as pd
import yaml
from mappings import CODIGO_MODALIDAD

def apply_schema_by_index(df: pd.DataFrame, yaml_path: str) -> pd.DataFrame:
    with open(yaml_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    cols = sorted(schema["columns"], key=lambda x: x["index"])

    idx = [c["index"] for c in cols]
    if len(idx) != len(set(idx)):
        raise ValueError("YAML inválido: índices repetidos")
    if idx != list(range(len(cols))):
        raise ValueError("YAML inválido: índices no consecutivos")

    if df.shape[1] != len(cols):
        raise ValueError("Número de columnas no coincide con el esquema")

    df = df.copy()
    df.columns = [c["name_technic"] for c in cols]

    for c in cols:
        col = c["name_technic"]
        t = c["type"].lower()

        if t == "string":
            df[col] = df[col].astype("string")

        elif t == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif t == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

        elif t == "datetime":
            fmt = c.get("format", "%d/%m/%Y")
            df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")

        elif t == "time":
            fmt = c.get("format", "%H:%M:%S")
            df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce").dt.time

        elif t == "timestamp":
            unit = c.get("unit", "ms")
            tz = c.get("tz", "UTC")
            scale = c.get("scale", 1)

            n = pd.to_numeric(df[col], errors="coerce")
            if scale and scale != 1:
                n = n / scale

            df[col] = pd.to_datetime(n, unit=unit, utc=True, errors="coerce")

            if tz and tz.upper() != "UTC":
                df[col] = df[col].dt.tz_convert(tz)

        else:
            raise ValueError(f"Tipo no soportado: {c['type']}")

    return df


def _add_ceco_column(df: pd.DataFrame) -> pd.DataFrame:
    """Añadir columna `ceco` con los 4 primeros caracteres de `centre_cost`.
    
    Convierte a entero cuando sea posible, mantiene cadena con letras.
    """
    if "centre_cost" not in df.columns:
        raise KeyError("Columna 'centre_cost' no encontrada en el DataFrame")

    # Convertir a string preservando nulos, tomar primeros 4 caracteres
    s = df["centre_cost"].astype("string").str[:4]

    # Intentar convertir a entero nullable cuando sea posible
    s_num = pd.to_numeric(s, errors="coerce").astype("Int64")

    # Usar el entero cuando esté disponible, si no dejar la cadena (p. ej. con letras)
    df["ceco"] = s_num.where(s_num.notna(), s)
    
    return df


def _add_cod_material_sinf_column(df: pd.DataFrame) -> pd.DataFrame:
    """Añadir columna `cod_material_sinf` con `material` sin el primer caracter."""
    if "material" not in df.columns:
        raise KeyError("Columna 'material' no encontrada en el DataFrame")

    df["cod_material_sinf"] = df["material"].astype("string").str[1:]
    
    return df


def _add_linea_actividad(df: pd.DataFrame) -> pd.DataFrame:
    """Añadir columna `linea_actividad` mapeando `cod_material_sinf` con CODIGO_MODALIDAD.
    
    Utiliza el mapping del módulo mappings para traducir códigos de material
    a modalidades diagnósticas. Si no encuentra el código, usa '-'.
    """
    if "cod_material_sinf" not in df.columns:
        raise KeyError("Columna 'cod_material_sinf' no encontrada en el DataFrame")

    df["linea_actividad"] = df["cod_material_sinf"].map(CODIGO_MODALIDAD).fillna("-")
    
    return df


def _safe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Preparar el DataFrame para exportación segura a Excel.
    
    Aplica las siguientes transformaciones:
    - Elimina timezones de columnas datetime (Excel no los soporta)
    - Convierte objetos time a string (Excel no soporta time sin fecha)
    """
    import datetime
    
    for col in df.columns:
        # Eliminar timezone de columnas datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
        
        # Convertir time objects a string (Excel no los soporta nativamente)
        elif df[col].dtype == 'object':
            # Verificar si contiene objetos time
            sample = df[col].dropna().head(1)
            if len(sample) > 0 and isinstance(sample.iloc[0], datetime.time):
                df[col] = df[col].astype(str)
    
    return df


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Aplicar transformaciones adicionales al DataFrame.

    Aquí se incluirán los scripts de transformación del dataframe (limpieza,
    normalización, creación de columnas derivadas, etc.).
    """
    # Transformación 1: crear columnas derivadas
    df = _add_ceco_column(df)
    df = _add_cod_material_sinf_column(df)
    df = _add_linea_actividad(df)
    
    # Transformación final: preparar para Excel
    df = _safe_for_excel(df)

    return df

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Procesar un archivo Excel usando el esquema gr55_schema.yaml"
    )
    parser.add_argument("input_path", help="Ruta del archivo a procesar (Excel)")
    parser.add_argument("--sheet", default="Data", help="Nombre de la hoja a leer (por defecto: Data)")
    parser.add_argument("--schema", default="gr55_schema.yaml", help="Ruta al archivo YAML de esquema")
    args = parser.parse_args()

    df = pd.read_excel(args.input_path, sheet_name=args.sheet, dtype=str)
    df_typed = apply_schema_by_index(df, args.schema)
    df_transformed = transform_dataframe(df_typed)
    df_transformed.to_excel("output.xlsx", index=False)
    print("terminado")
