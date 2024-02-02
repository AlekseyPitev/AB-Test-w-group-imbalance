import pandas as pd
from urllib.parse import urlencode
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
from datetime import timedelta

# ссылки для загрузки с ЯД:
base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
dop_path = requests.get(base_url + urlencode(dict(public_key='https://disk.yandex.ru/d/5Kxrz02m3IBUwQ'))).json()['href']
osn_path = requests.get(base_url + urlencode(dict(public_key='https://disk.yandex.ru/d/UhyYx41rTt3clQ'))).json()['href']
stud_path = requests.get(base_url + urlencode(dict(public_key='https://disk.yandex.ru/d/Tbs44Bm6H_FwFQ'))).json()['href']
check_path = requests.get(base_url + urlencode(dict(public_key='https://disk.yandex.ru/d/pH1q-VqcxXjsVA'))).json()['href']

default_args = {
    'owner': 'a-pitev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 15)}

@dag(default_args=default_args, schedule_interval = '15 17 * * *', catchup=False)
def a_pitev_upload_nd_recalc():
    @task()
    def metcalcnewdata(dop_path, osn_path, stud_path, check_path, metrix=None):
        'Функция считывает 4 датасета и пересчитыватет метрики'

        try: df_dop = pd.read_csv(dop_path)
        except FileNotFoundError: print("Файл не найден")

        # Проверка структуры данных
        if len(df_dop.columns) != 2 or df_dop.dtypes.nunique() != 2:
            print("Ошибка структуры данных")
        id_col = df_dop.select_dtypes(include=['int', 'int64']).columns.tolist()
        grp_col = df_dop.select_dtypes(include=['object']).columns.tolist()
        if len(id_col) != 1 or len(grp_col) != 1:
            print("Ошибка структуры данных")
        df_dop.rename(columns={id_col[0]: 'id', grp_col[0]: 'grp'}, inplace=True)

    #     загрузка прочих данных
        try: groupdf = pd.read_csv(osn_path, sep = ';')
        except FileNotFoundError: print("Файл не найден")
        try: studdf = pd.read_csv(stud_path)
        except FileNotFoundError: print("Файл не найден")
        try: checkdf = pd.read_csv(check_path, sep = ';')
        except FileNotFoundError: print("Файл не найден")

    #     добавляем поля и соединяем данные о гр польз-лей
        df_dop['dop'] = 1
        groupdf['dop'] = 0
        df = groupdf.append(df_dop, ignore_index=True)
        df['grp_dig'] = df['grp'].apply(lambda x: 0 if x == 'A' else 1)

    #     соединим с данными об активности и покупках польз-лей
        checkdf = checkdf.rename(columns={'student_id': 'pay_id'})
        t2 = studdf.merge(checkdf, how = 'left', left_on='student_id', right_on='pay_id')
        t2['rev'].fillna(0, inplace=True)
        df = df.merge(t2, how = 'left', left_on='id', right_on='student_id')
        df['active'] = df['student_id'].apply(lambda x: 0 if pd.isna(x) else 1)
        df['is_check'] = df['pay_id'].apply(lambda x: 0 if pd.isna(x) else 1)
        df.rename(columns={'student_id': 'act_id'}, inplace=True)
    
    #     считаем метрики только для основной части данных
        df_by_gr_base = df[df['dop'] == 0].groupby('grp', as_index = False). \
                        agg(grp_dig = ('grp_dig', 'mean'), 
                            stud_count = ('id', 'count'), 
                            CR_act = ('active', 'mean'), 
                            act_count = ('act_id', 'count'), 
                            CR_pay = ('is_check', 'mean'), 
                            pay_count = ('pay_id', 'count'),
                            rev_sum = ('rev', 'sum'))

    #     считаем метрики на всех данных
        df_by_gr = df.groupby('grp', as_index = False).agg(grp_dig = ('grp_dig', 'mean'), stud_count = ('id', 'count'), 
                                            CR_act = ('active', 'mean'), act_count = ('act_id', 'count'), 
                                            CR_pay = ('is_check', 'mean'), pay_count = ('pay_id', 'count'),
                                           rev_sum = ('rev', 'sum'))    
    #     производные метрики считаем только если их надо выводить (в зависимости от параметров ф-ции)
        if metrix is None or 'CR_pay_act' in metrix: 
            df_by_gr_base['CR_pay_act'] = df_by_gr_base['pay_count'] / df_by_gr_base['act_count']
            df_by_gr['CR_pay_act'] = df_by_gr['pay_count'] / df_by_gr['act_count']

        if metrix is None or 'ARPU' in metrix:
            df_by_gr_base['ARPU'] = df_by_gr_base['rev_sum'] / df_by_gr_base['stud_count']
            df_by_gr['ARPU'] = df_by_gr['rev_sum'] / df_by_gr['stud_count']

        if metrix is None or 'ARPAU' in metrix:
            df_by_gr_base['ARPAU'] = df_by_gr_base['rev_sum'] / df_by_gr_base['act_count']
            df_by_gr['ARPAU'] = df_by_gr['rev_sum'] / df_by_gr['act_count']

        if metrix is None or 'ARPPU' in metrix:
            df_by_gr_base['ARPPU'] = df_by_gr_base['rev_sum'] / df_by_gr_base['pay_count']
            df_by_gr['ARPPU'] = df_by_gr['rev_sum'] / df_by_gr['pay_count']

        df_by_gr_base['data_type'] = 'base'
        df_by_gr['data_type'] = 'all'
        df_by_gr = df_by_gr_base.append(df_by_gr, ignore_index=True)

        if metrix is None:
            print(df_by_gr)
        else:
            col2show = ['grp', 'data_type'] + [col for col in metrix if col in df_by_gr.columns]
            print(df_by_gr[col2show])
        
    metcalcnewdata(dop_path, osn_path, stud_path, check_path, ['CR_pay', 'ARPU', 'ARPPU'])
a_pitev_upload_nd_recalc = a_pitev_upload_nd_recalc()