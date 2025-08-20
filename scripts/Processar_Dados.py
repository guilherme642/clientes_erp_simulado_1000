import re
import pandas as pd
from unidecode import unidecode
import os
import psycopg2

class DataProcessorRefactored:

    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.df = self.excel_to_dataframe()
        self.uf_map = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

    def excel_to_dataframe(self) -> pd.DataFrame:
        try:
            self.df = pd.read_excel(self.excel_path)
            return self.df
        except FileNotFoundError:
            print(f"Error: The file '{self.excel_path}' was not found.")
            return None

    def remove_extra_info(self) -> pd.DataFrame:
        self.df.drop(["Observações", "Extra Info"], axis=1, inplace=True)

    def standardize_case(self) -> pd.DataFrame:
        self.df['UF'] = self.df['UF'].astype(str).str.upper()
        self.df['Nome completo'] = self.df['Nome completo'].astype(str).str.upper()

    def standardize_phone_number(self):
        def format_number(phone_str):
            regex_digitos = re.findall("[0-9]", phone_str)
            while len(regex_digitos) >= 11:
                regex_digitos.pop(0)
            if len(regex_digitos) < 11:
                regex_digitos.insert(0, "(")
                regex_digitos.insert(3, ")")
                regex_digitos.insert(4, "9")
            return "".join(regex_digitos)
        self.df['Contato'] = self.df['Contato'].apply(format_number)
            
    def remove_duplicates(self):
        self.df.drop_duplicates(inplace=True)

    def remove_prefix_and_accents(self):
        def clean_name(name_str):
            name_str = str(name_str)
            if re.search(r"[+.]", name_str):
                name_str = re.split(r"[+.]", name_str)[1].lstrip()
            
            name_str = re.sub(r"[^a-zA-Z\s\u00C0-\u00FF]", " ", name_str)
            return unidecode(name_str.lstrip()).strip()

        self.df['Nome completo'] = self.df['Nome completo'].apply(clean_name)
    
    def split_full_name(self):
        nome_value = []
        sobrenome_value = []
        for row in self.df.values.tolist():
            nome_row = []
            sobrenome_row = [] 
            sub_strings = row[0].split()
            # 3 Strings com conjunção
            if len(sub_strings) == 3 and (len(sub_strings[1]) <= 3 and len(sub_strings[1]) > 1 ):
                nome_row.append(sub_strings[0])
                sobrenome_row.append(sub_strings[1])
                sobrenome_row.append(sub_strings[2])
            # 2 Strings sem conjunção
            elif len(sub_strings) == 2:
                nome_row.append(sub_strings[0])
                sobrenome_row.append(sub_strings[1])
            # 3 Strings sem conjunção
            elif len(sub_strings) == 3 and len(sub_strings[1]) > 3:
                nome_row.append(sub_strings[0])
                nome_row.append(sub_strings[1])
                sobrenome_row.append(sub_strings[2])
            # 4 Strings com conjunção
            elif len(sub_strings) == 4:
                nome_row.append(sub_strings[0])
                nome_row.append(sub_strings[1])
                sobrenome_row.append(sub_strings[2])
                sobrenome_row.append(sub_strings[3])
            nome_value.append(" ".join(nome_row))
            sobrenome_value.append(" ".join(sobrenome_row))
        self.df.insert(0, "Nome", nome_value, True)
        self.df.insert(1, "Sobrenome", sobrenome_value, True)
        self.df.drop("Nome completo", axis = 1, inplace = True)

    def standardize_dates(self):
        def format_date(date):
            try:
                date = pd.to_datetime(date)
                return date.strftime("%Y-%m-%d")
            except:
                return "invalido"
        self.df['Data Cadastro'] = self.df['Data Cadastro'].apply(format_date)

    def add_origin_column(self):
        origin = []
        for row in self.df.values.tolist():
            origin.append(excel_file)

        self.df.insert(5, "Arquivo de Origem", origin, True)

    def add_client_status(self):
        base_df = pd.read_excel(self.excel_path)
        
        client_stat = []
        for row in base_df.values.tolist():
            try:
                if re.search("VIP", row[4]):
                    row[4] = "VIP"
                else:
                    row[4] = "REGULAR"
            except:
                row[4] = "REGULAR"
            finally:
                client_stat.append(row[4])
        
        self.df.insert(4, "Status do Cliente", client_stat, True)

    def add_uuid_column(self):
        hash_cliente = []

        for row in self.df.values.tolist():
            if type(row[2]) != float:
                hash_cliente.append(abs(hash(row[2])))
        
        self.df.insert(0, 'Hash Cliente', hash_cliente)

    def generate_rejection_logs(self):
        col_status = self.df.columns[6]
        col_uf = self.df.columns[4] 
        
        rejeitadas = self.df[(self.df[col_status] == "invalido") | (~self.df[col_uf].isin(self.uf_map))]

        aceitos = self.df[(self.df[col_status] != "invalido") & (self.df[col_uf].isin(self.uf_map))]
        
        processed_dir = "./processed_data"

        os.makedirs(processed_dir, exist_ok=True)
        rejeitadas.to_csv(f"{processed_dir}/erros.csv", index=False)
        aceitos.to_csv(f"{processed_dir}/clientes_erp_formatado.csv", index=False)

    def load_csv_to_postgres(self, csv_path):
        conn = psycopg2.connect(
            dbname="erp",
            user="postgres",
            password="1939",
            host="localhost",  # ou o nome do container se rodar fora do host
            port="5432"
        )
        cur = conn.cursor()

        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO clientes (hash, nome, sobrenome, contato, uf, status, data_cadastro, origem)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (hash) DO NOTHING
            """, tuple(row))

        conn.commit()
        cur.close()
        conn.close()
        print("Dados inseridos no banco com sucesso!")
        
    def run_all_steps(self):
        if self.df is not None:
            self.remove_extra_info()
            self.standardize_case()
            self.standardize_phone_number()
            self.remove_duplicates()
            self.remove_prefix_and_accents()
            self.split_full_name()
            self.standardize_dates()
            self.add_origin_column()
            self.add_client_status()
            self.add_uuid_column()
            self.generate_rejection_logs()
            self.load_csv_to_postgres("./processed_data/clientes_erp_formatado.csv")
            print("Processamento concluido, olhe a pasta [processed_data]")

if __name__ == "__main__":
    excel_file = "./raw_data/clientes_erp_simulado_1000.xlsx"
    processor = DataProcessorRefactored(excel_file)
    processor.run_all_steps()