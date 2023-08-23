import pandas as pd
import luigi
import os
import shutil

ruta_salida1 = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "data.xlsx")

ruta = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data")

ruta_salida2 = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "last_version.txt")

# Obtener la lista de archivos con prefijo "data_v" y extensión ".xlsx"
existing_versions = [
    f for f in os.listdir(ruta) if f.startswith("data_v") and f.endswith(".xlsx")
]

# Obtener el último número de versión existente
last_version = max([int(f.split("_v")[1].split(".xlsx")[0]) for f in existing_versions], default=0)

print("Tenga en cuenta que la ultima versión era la:")
print(last_version)

# Crear una copia de data.xlsx con el número de versión
if os.path.exists(ruta_salida1):
    last_version += 1
    new_filename = f"data_v{last_version}.xlsx"
    new_filepath = os.path.join(ruta, new_filename)
    shutil.copy(ruta_salida1, new_filepath)
    print(f"Se ha creado una copia de {ruta_salida1} como {new_filename}")

    # Actualizar el archivo last_version.txt
    with open(ruta_salida2, 'w') as f:
        f.write(str(last_version))
else:
    print(f"No se encontró el archivo {ruta_salida1}")


os.remove(ruta_salida1)


class Autodiagnostico(luigi.Task):

    ruta_preg = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__),"..")),"data","DiccionarioPreguntas.xlsx")

    ruta_data = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "DiagnosticoResults.xlsx")

    ruta_salida = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "data.xlsx")

    def output(self):
        return luigi.LocalTarget(self.ruta_salida)

    def run(self):

        preg = pd.read_excel(self.ruta_preg)
        data = pd.read_excel(self.ruta_data)

        col_noPreg = ['respondent_id', 'collector_id', 'date_created', 'date_modified', 'ip_address', 'email_address',
                      'first_name', 'last_name', 'custom_2', 'PAIS', 'PAIS2', 'CIUDAD', 'CIUDAD 2', 'GRUPO',
                      'INICIO O FIN',
                      'NOMBRE EMPRESA', 'SECTOR ECONÓMICO', 'NOMBRE', 'EMAIL', 'TIPO DE IDENTIFICACIÓN',
                      'NUMERO IDENTIFICACIÓN',
                      'GENERO', 'FECHA NACIMIENTO', 'NIVEL JERARQUIA', 'NIVEL JERARQUIA2', 'CARGO', 'ANTIGÜEDAD',
                      'NIVEL EDUCACION',
                      'SEGUNDO INDIOMA', 'SEGUNDO IDIOMA2', 'DOMINIO', 'AREAS MAYOR INNOVACION',
                      'AREAS MAYOR INNOVACION2']

        data_melted = pd.melt(data, id_vars=col_noPreg, var_name='Pregunta', value_name='Valor')

        data_melted = data_melted.merge(preg, on='Pregunta', how='left')

        data_melted.head()

        # Corregimos la jerarquia
        jerarquia = []

        for i in range(0, len(data_melted)):
            if (data_melted['NIVEL JERARQUIA'][i] != '') or (data_melted['NIVEL JERARQUIA'][i] != ' '):
                jerarquia.append(data_melted['NIVEL JERARQUIA'][i])
            else:
                jerarquia.append(data_melted['NIVEL JERARQUIA2'][i])

        data_melted['jerarquia_final'] = jerarquia

        # Exportar el DataFrame resultante a un archivo Excel
        data_melted.to_excel(self.ruta_salida, index=False)


if __name__ == '__main__':
    luigi.build([Autodiagnostico()], local_scheduler=True)






