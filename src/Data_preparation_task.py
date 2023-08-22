import pandas as pd
import luigi
import os


class RenameAndRemoveTask(luigi.Task):

    original_file=os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "data.xlsx")

    ruta_salida = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data")

    def output(self):
        renamed_file = os.path.join(self.ruta_salida, "data_")
        return luigi.LocalTarget(renamed_file)

    def run(self):
        renamed_file_path = self.output().path
        os.rename(self.original_file, renamed_file_path)


class Autodiagnostico(luigi.Task):

    ruta_preg = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__),"..")),"data","DiccionarioPreguntas.xlsx")

    ruta_data = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "DiagnosticoResults.xlsx")

    ruta_salida = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "data", "data.xlsx")

    def requires(self):
        return{

            'rename&remove': RenameAndRemoveTask()
        }



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
    luigi.build([Autodiagnostico()], local_scheduler = True)






