from datetime import datetime
import logging
import json
import os
import psycopg2
import re
from psycopg2 import pool

# Logger settings CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)




def lambda_handler(event, context):
    """
    Function that's convert the json data receive into a python dictionary
    @param json_data: json received from client
    """

    try:
        list_erros = []
        measurement = []
        processes = []
        utility = []
        manufactured = []
        filter_processes = []
        filter_utility = []
        filter_measurement = []
        conn, pool_conn = connect_postgres()
        equipments = get_equipment(conn)
        products = get_product(conn)
        result = table_type(conn)
        count = 0
        if result:

            for e in event:
                if 'value_active' in e:
                    data = measurement_to_utility(e)

                    for d in data:
                        response = data_handler(d, count, conn, equipments, products)
                        count += 1
                        if 'table' not in  response:
                            list_erros.append(response) 
                        else:
                            table = response['table']
                            if table == 'utility':
                                utility.append(response)
                            elif table == 'processes':
                                processes.append(response)
                            elif table == 'manufactured':
                                manufactured.append(response)
                            elif table == 'filter_processes':
                                filter_processes.append(response) 
                            elif table == 'filter_utility':
                                filter_utility.append(response)
                            else:
                                filter_measurement.append(response)

                
                else:
                    response = data_handler(d, count, conn, equipments, products)
                    count += 1
                    if 'table' not in  response:
                        list_erros.append(response)
                    else:
                        table = response['table']
                        if table == 'utility':
                            utility.append(response)
                        elif table == 'processes':
                            processes.append(response)
                        elif table == 'manufactured':
                            manufactured.append(response)
                        elif table == 'filter_processes':
                            filter_processes.append(response) 
                        elif table == 'filter_utility':
                            filter_utility.append(response)
                        else:
                            filter_measurement.append(response)
        else:
            for e in event:
               response = data_handler(e, count, conn, equipments, products)
               count += 1
               if 'table' not in  response:
                   list_erros.append(response)
               else:
                   table = response['table']
                   if table == 'measurement':
                       measurement.append(response)
                   elif table == 'utility':
                       utility.append(response)
                   elif table == 'processes':
                       processes.append(response)
                   elif table == 'manufactured':
                       manufactured.append(response)
                   elif table == 'filter_processes':
                       filter_processes.append(response) 
                   elif table == 'filter_utility':
                       filter_utility.append(response)
                   else:
                       filter_measurement.append(response)

        if measurement:
            insert_measurement(measurement, conn)
        if utility:
            insert_utility(utility, conn)
        if processes:
            insert_processes(processes, conn)
        if manufactured:
            insert_production(manufactured, conn)
        if filter_processes:
            insert_processes_filters(filter_processes, conn)
        if filter_utility:
            insert_utility_filters(filter_utility, conn)
        if filter_measurement:
            insert_measurement_filters(filter_measurement, conn)

        if pool_conn is not None:
            pool_conn.closeall()


        if list_erros:
            # insert_log_errors(list_erros)
            return {
            'statusCode': 400,
            'body':json.dumps(list_erros, indent=2, ensure_ascii=False)
            }
        else:
            return {
            'statusCode': 200,
            'body':json.dumps({'message':'Inserção sem erros'}, indent=2, ensure_ascii=False)
            }

    except Exception as e:
        logging.error(e)



def data_handler(event, count, conn, equipments, products):
    """
    Initial proccess.
    @param event: Message received.
    """
    logger.info("Received event: " + json.dumps(event, indent=2))
    event['index'] = count


    SECRET_KEY = "EHu2wf3M0!qA9NEJmUQBpdG^34Z06"

    if 'token' in event.keys(): 
        if event ["token"] != SECRET_KEY :
            return {
            'statusCode': 403,
            'body': json.dumps('Not Authorized')
        }


    event_validate = data_validate(event, conn)

    if event_validate == True:

        data = get_data(event, count, conn, equipments, products)

        return data

    else:
        return event_validate


def measurement_to_utility(data):

    company = get_company()

    list_data = []
    keys = [ d for d in data.keys() if d not in ['capture_id', 'datetime_read', 'token']]
    
    for k in keys:
        cap = data['capture_id']+' - '+k

        if 'token' in data.keys():
            list_data.append({'token':data['token'] ,'capture_id':cap, 'datetime_read':data['datetime_read'], 'value':data[k]})
        
        else:
            list_data.append({'capture_id':cap, 'datetime_read':data['datetime_read'], 'value':data[k]})

    if company == 'mrn':
        return list_data[:8]

    else:
        return list_data



def get_company():

    conn = connect_postgres()

    cur = conn.cursor()

    try:
        sql = ("""
                SELECT
                report_db
                FROM company

        """)

        cur.execute(sql)
        company = cur.fetchone()
        cur.close()
        if company is None:
            raise Exception
        company = company[0]

        logger.info("company obtained in PostgreSql.")

        return company

    except Exception as error:
        company = None

        logger.error("company not found: {}".format(company))



def data_validate(event, conn):

    if 'f_value' in event:
        return static_validate(event, conn)  
    elif 'value_active' in event or 'value_reactive' in event:
        return measurement_validate(event)
    elif 'product' in event or len(event) == 5:
        return production_validate(event)
    elif 'p_value' in event:
        return processes_validate(event)
    else:
        return utility_validate(event)


def get_col_type(conn):

    cols = get_table_columns(conn)

    col_types = {}

    for c in cols:
        for i in cols[c]:
            col_types[i] = cols[c][i]['type']

    return col_types


def get_table(capture_id, conn):

    cols = get_table_columns(conn)

    for c in cols:
        if capture_id in cols[c].keys():
            return c

    return ''


def static_validate(event, conn):

    errors = {}

    col_type = get_col_type(conn)

    list_keys = [
        "index",
        "capture_id",
        "datetime_read",
        "f_value"
    ]

    if 'capture_id' in event and  type(event['capture_id']) is not str:
        capture_id = event['capture_id']
        errors['capture_id'] = f"{capture_id} não é do tipo 'string'"

    if 'datetime_read' in event:

        date = event['datetime_read']

        if type(date) == str:
            
            regex = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}', date)
            
            if regex is None:
                errors['datetime_read'] = f'A data {date} está fora do padrão ISO 8601 2020-01-03T00:00:00-03:00'

        else:
            errors['datetime_read'] = f"'{date}' não é do tipo 'string'"

    if 'capture_id' in event:
        if 'f_value' in event: 
            if col_type[event['capture_id']] == 'text' and type(event['f_value']) is not str:
                value = event['f_value']
                errors['f_value'] = f"'{value}' não é do tipo 'string'"
            elif col_type[event['capture_id']] == 'float' and type(event['f_value']) is not float:
                value = event['f_value']
                errors['f_value'] = f"'{value}' não é do tipo 'float'"                    

    
    event_keys = [ k for k in event.keys()]

    invalid_keys = [ k for k in event.keys() if k not in list_keys ]

    forgotten_keys = [ k for k in list_keys if k not in event_keys ]

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'

    if errors:
        errors['index'] = event['index']

        return errors


    return True



def utility_validate(event):

    errors = {}

    list_keys = [
        "index",
        "capture_id",
        "datetime_read",
        "value"
    ]

    if 'capture_id' in event and  type(event['capture_id']) is not str:
        capture_id = event['capture_id']
        errors['capture_id'] = f"{capture_id} não é do tipo 'string'"

    if 'datetime_read' in event:

        date = event['datetime_read']

        if type(date) == str:
            
            regex = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}', date)
            
            if regex is None:
                errors['datetime_read'] = f'A data {date} está fora do padrão ISO 8601 2020-01-03T00:00:00-03:00'

        else:
            errors['datetime_read'] = f"'{date}' não é do tipo 'string'"

    if 'value' in event and type(event['value']) is not float:
        value = event['value']

        errors['value'] = f"'{value}' não é do tipo 'number'"
    
    event_keys = [ k for k in event.keys()]

    invalid_keys = [ k for k in event.keys() if k not in list_keys ]

    forgotten_keys = [ k for k in list_keys if k not in event_keys ]

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'

    if errors:
        errors['index'] = event['index']

        return errors


    return True

def get_table_columns(conn):


    sql = """
        SELECT 
            value
        FROM
            system_config
        WHERE
            key = 'staticTable'
    """

    cols = {}
        
    try:
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchone()
        data = json.loads(data[0])

        for d in data:
            columns = {}
            for i in data[d]: 
                tmp = {}
                tmp['column'] = i
                tmp['type'] = data[d][i]['type']
                columns[data[d][i]['capture_id']] = tmp
            cols[d] = columns
            
    except Exception as error:
        print(error)
    finally:
        return cols

def get_tables_capture_id(conn):

    cols = get_table_columns(conn)

    capture_ids = []

    for c in cols:
        for i in cols[c]:
            capture_ids.append(i)

    return capture_ids

def processes_validate(event):

    errors = {}

    list_keys = [
        "index",
        "capture_id",
        "datetime_read",
        "p_value"
    ]

    if 'capture_id' in event and  type(event['capture_id']) is not str:
        capture_id = event['capture_id']
        errors['capture_id'] = f"{capture_id} não é do tipo 'string'"

    if 'datetime_read' in event:

        date = event['datetime_read']

        if type(date) == str:
            
            regex = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}', date)
            
            if regex is None:
                errors['datetime_read'] = f'A data {date} está fora do padrão ISO 8601 2020-01-03T00:00:00-03:00'

        else:
            errors['datetime_read'] = f"'{date}' não é do tipo 'string'"

    if 'p_value' in event and type(event['p_value']) is not float:
        value = event['p_value']

        errors['p_value'] = f"'{value}' não é do tipo 'number'"
    
    event_keys = [ k for k in event.keys()]

    invalid_keys = [ k for k in event.keys() if k not in list_keys ]

    forgotten_keys = [ k for k in list_keys if k not in event_keys ]

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'

    if errors:
        errors['index'] = event['index']

        return errors


    return True


def production_validate(event):

    errors = {}

    list_keys = [
        "index",
        "capture_id",
        "datetime_read",
        "value",
        "product"
    ]

    if 'capture_id' in event:
        if type(event['capture_id']) is not str:
            capture_id = event['capture_id']
            errors['capture_id'] = f"{capture_id} não é do tipo 'string'"
        else:
            capture_id = event['capture_id']
            regex = re.search(r'^([\d|\w]+)_([\d|\w]+)_([\d|\w]+)$', capture_id)
            if not regex:
                errors['capture_id'] = f'{capture_id} não possui formato padrão para o campo (COD-UNIDADE_COD-LINHA_COD-PRODUTO)'


    if 'datetime_read' in event:

        date = event['datetime_read']

        if type(date) == str:
            
            regex = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}', date)
            
            if regex is None:
                errors['datetime_read'] = f'A data {date} está fora do padrão ISO 8601 2020-01-03T00:00:00-03:00'

        else:
            errors['datetime_read'] = f"'{date}' não é do tipo 'string'"

    if 'value' in event and type(event['value']) is not float and type(event['value']) is not int:
        value = event['value']
        errors['value'] = f"'{value}' não é do tipo 'number'"

    if 'product' in event and type(event['product']) is not str:
        product = event['product']
        errors['product'] = f"'{product}' não é do tipo 'string'"
    

    event_keys = [ k for k in event.keys()]

    invalid_keys = [ k for k in event.keys() if k not in list_keys ]

    forgotten_keys = [ k for k in list_keys if k not in event_keys ]

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'

    if errors:
        errors['index'] = event['index']

        return errors

    return True



def measurement_validate(event):

    errors = {}

    list_keys = [   
        "index",     
        "capture_id",
        "datetime_read",
        "value_active",
        "value_reactive",
        "tension_phase_neutral_a",
        "tension_phase_neutral_b",
        "tension_phase_neutral_c",
        "current_a",
        "current_b",
        "current_c",
        "thd_tension_a",
        "thd_tension_b",
        "thd_tension_c",
        "thd_current_a",
        "thd_current_b",
        "thd_current_c"
    ]

    if 'capture_id' in event and type(event['capture_id']) is not str:

        capture_id = event['capture_id']

        errors['capture_id'] = f"{capture_id} não é do tipo 'string'"


    if 'datetime_read' in event:

        date = event['datetime_read']

        if type(date) == str:
            
            regex = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}', date)
            
            if regex is None:
                errors['datetime_read'] = f'A data {date} está fora do padrão ISO 8601 2020-01-03T00:00:00-03:00'

        else:
            errors['datetime_read'] = f"'{date}' não é do tipo 'string'"


    event_keys = [ k for k in event.keys()]

    invalid_keys = [ k for k in event.keys() if k not in list_keys ]

    forgotten_keys = [ k for k in list_keys if k not in event_keys ]

    values_keys = [ k for k in list_keys if k in event_keys and k not in ['capture_id', 'datetime_read', 'index']]


    for v in values_keys:
        value = event[v]
        if type(value) is not float and type(value) is not int:
            errors[v] = f"'{value}' não é do tipo 'number'"

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'
    
    if errors:
        errors['index'] = event['index']

        return errors

    return True



def connect_postgres():
    """
    Establish connect with PostgreSql
    @param data: Message received
    @return: Connection
    """

    conn = None
    threaded_postgreSQL_pool = None
    try:
        rds_host = os.environ.get('RDS_HOST')
        rds_username = os.environ.get('RDS_USERNAME')
        rds_user_pwd = os.environ.get('RDS_USER_PWD')
        rds_db_name = os.environ.get('RDS_DATABASE')

        threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, user=rds_username,
                                                         password=rds_user_pwd,
                                                         host=rds_host,
                                                         database=rds_db_name)

        if threaded_postgreSQL_pool:
            conn = threaded_postgreSQL_pool.getconn()
            logger.info("Connected with Postgres.")

    except ConnectionError as error:
        conn = None
        logger.error("Connecting with Postgres: ", error)

    return conn, threaded_postgreSQL_pool 



def table_type(conn):
    """
    Get the information, if the company will use 1 or 2 tables.
    @param conn: Connection with PostgreSql
    @return: .
    """
    cur = conn.cursor()

    try:
        sql = ("""

        SELECT
        value
        FROM system_config
        WHERE key = 'generalConfig'

        """)

        cur.execute(sql)
        result = cur.fetchone()
        cur.close()
        if result is None:
            raise Exception
        result = result[0]

        logger.info("result obtained in PostgreSql.")

    except Exception as error:
        result = None

        logger.error("result not found")

    if 'true' in result:
        return True

    else:
        return False



def set_period(event):
    """
    Set the period what stands for the number of intervals of 15 minutes between the datetime_read and the initial date of the month
    @param event: Message received
    """
    datetime_read = datetime.strptime(event['datetime_read'], "%Y-%m-%dT%H:%M:%S%z")
    year = datetime_read.year
    month = datetime_read.month
    day = datetime_read.day

    datetime_read = datetime.strptime(datetime_read.strftime("%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S")
    initial_date = datetime(year, month, day, 0, 0, 0)

    diff = datetime_read - initial_date
    minutes = diff.total_seconds() / 60
    intervals = minutes / 15
    period = round(intervals)

    return period



def get_data(event, count, conn, equipments, products):
    """
    Extract the data from the message received.
    @param event: Message received.
    @return: Dictionary with datas.
    """
    error = {}
    equipment = None
    table = None
    product = None
    try:
        if 'f_value' in event:
            table = get_table(event['capture_id'], conn)  
        elif 'product' in event:
                if event['capture_id'] in products.keys():
                    product = products[event['capture_id']]
                if product is None:
                    line = event['capture_id'].split('_')[1]
                    product_table = get_product_line(conn)

                    if product_table is not None:
                        line_id = get_line_id(product_table, line, conn)

                        if line_id is not None:
                            if create_product(event, conn):
                                product = get_product(event, conn)
                                insert_product_relation(product_table, product, line_id, conn)
                        else:
                            return {
                            "index":count,
                            'error':f"A linha  '{line}' não está presente no banco de dados"
                        }

                    else:
                        return {
                            "index":count,
                            'error':f"A linha  '{line}' não está presente no banco de dados"
                        }

        else:
            if event['capture_id'] in equipments.keys():
                equipment = equipments[event['capture_id']]
            

        if equipment or table or product:

            if 'value_active' in event:
            # Datetime register is the current datetime.

                period = set_period(event)
                
                data = {
                    'index':event['index'],
                    'capture_id': event['capture_id'],
                    'datetime_register': datetime.now().isoformat(),
                    'datetime_read': event['datetime_read'],
                    'value_active': event['value_active'],
                    'value_reactive': event['value_reactive'],
                    'period': period,
                    'tension_phase_neutral_a': event['tension_phase_neutral_a'], 
                    'tension_phase_neutral_b': event['tension_phase_neutral_b'], 
                    'tension_phase_neutral_c': event['tension_phase_neutral_c'], 
                    'current_a': event['current_a'], 
                    'current_b': event['current_b'], 
                    'current_c': event['current_c'],
                    'thd_tension_a': event['thd_tension_a'],
                    'thd_tension_b': event['thd_tension_b'],
                    'thd_tension_c': event['thd_tension_c'],
                    'thd_current_a': event['thd_current_a'],
                    'thd_current_b': event['thd_current_b'],
                    'thd_current_c': event['thd_current_c'],
                    'plant_equipment_id':equipment,
                    'table':'measurement'   
                }
            
            elif 'product' in event:

                data = {
                    'index':event['index'],
                    'capture_id':event['capture_id'],
                    'datetime_read':event['datetime_read'],
                    'value':event['value'],
                    'product':product,
                    'table':'manufactured'
                }

            elif 'p_value' in event:

                data = {
                    'index':event['index'],
                    'capture_id': event['capture_id'],
                    'datetime_register': datetime.now().isoformat(),
                    'datetime_read': event['datetime_read'],
                    'p_value': event['p_value'],
                    'plant_equipment_id':equipment,
                    'table':'processes'  
                }

            elif 'f_value' in event:
                if table == 'processes':

                    data = {
                        'index':event['index'],
                        'capture_id': event['capture_id'],
                        'datetime_read': event['datetime_read'],
                        'f_value': event['f_value'],
                        'table':'filter_processes'  
                    }

                elif table == 'utility':

                    data = {
                        'index':event['index'],
                        'capture_id': event['capture_id'],
                        'datetime_read': event['datetime_read'],
                        'f_value': event['f_value'],
                        'table':'filter_utility'  
                    }

                else:
                    data = {
                        'index':event['index'],
                        'capture_id': event['capture_id'],
                        'datetime_read': event['datetime_read'],
                        'f_value': event['f_value'],
                        'table':'filter_measurement'  
                    }                    


            else:
                data = {
                    'index':event['index'],
                    'capture_id': event['capture_id'],
                    'datetime_register': datetime.now().isoformat(),
                    'datetime_read': event['datetime_read'],
                    'value': event['value'],
                    'plant_equipment_id':equipment,
                    'table':'utility'  
                }
        
        else:
            if 'product' in event:
                return {
                    'index':count,
                    'error':f'Produto {event["capture_id"]} não encontrado'
                }
            else:
                return {
                    'index':count,
                    'error':f'Medidor {event["capture_id"]} não encontrado'
                }


    except Exception as error:
        data = {}
        logger.error("Converting data: ", error)

    return data



def get_equipment(conn):
    """
    Get the equipment, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Equipment.
    """

    try:
        cur = conn.cursor()
        sql = (
            "SELECT id_capture, plant_equipment_id "
            "FROM plant_equipment "
            "WHERE id_capture is not null"
        )
        cur.execute(sql)
        equipment = dict(cur.fetchall())
        cur.close()
        if len(equipment) == 0:
            logger.info("There is no equipment in database")

            return None

        logger.info("Equipments obtained in PostgreSql.")

    except Exception as error:
        equipment = None

        logger.error("Error: {}".format(error))

    finally:
        return equipment



def get_product(conn):
    """
    Get the product, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Product.
    """

    try:
        cur = conn.cursor()
        sql = (
            "SELECT id_capture, product_id "
            "FROM product "
            "WHERE id_capture is not null"
        )
        cur.execute(sql)
        product = dict(cur.fetchall())
        cur.close()
        if len(product) == 0:
            logger.info("There is no product in database.")
            return None
        logger.info("Products obtained in PostgreSql.")

    except Exception as error:
        product = None

        logger.error("Error: {}".format(error))

    return product


def get_product_line(conn):

    if check_product_point(conn):
        return 'product_point'
    elif check_product_sector(conn):
        return 'product_sector'
    elif check_product_company(conn):
        return 'product_company'
    else:
        return None
    


def check_product_company(conn):

    row = 0

    sql = """
        SELECT
        id
        FROM product_company
        LIMIT 1
    """

    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.rowcount
        cur.close()
    except Exception as error:
        print(error)



def check_product_point(conn):

    row = 0

    sql = """
        SELECT
        id
        FROM product_point
        LIMIT 1
    """

    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.rowcount
        cur.close()
    except Exception as error:
        print(error)
    finally:
        return row


def check_product_sector(conn):

    row = 0

    sql = """
        SELECT
        id
        FROM product_sector
        LIMIT 1
    """

    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.rowcount
        cur.close()
    except Exception as error:
        print(error)
    finally:
        return row


def get_line_id(product_table, line, conn):


    line_id = 0

    if 'point' in product_table:
        table_id = 'plant_point_id'
        table = 'plant_point'

    elif 'sector' in product_table:
        table_id = 'plant_sector_id'
        table = 'plant_sector'
    else:
        table_id = 'company_id'
        table = 'company'

    if 'company' not in product_table:

        sql = f"""
            SELECT
            {table_id}
            FROM {table}
            WHERE name = '{line}'
        """

    else:
        sql = f"""
            SELECT
            c.company_id
            FROM company c
            LEFT JOIN company_division d ON c.company_division_id = d.company_division_id
            WHERE d.name = '{line}'
        """

    try:
        cur = conn.cursor()
        cur.execute(sql)
        line_id = cur.fetchone()
        if line_id is not None:
            line_id = line_id[0]
        cur.close()

    except Exception as error:
        print(error)

    finally:
        return line_id


def insert_log_errors(list_error, conn):


    error = []

    for l in list_error:
        error.append({
            'error':json.dumps(l, ensure_ascii=False)
        })

    datetime_register = datetime.now().isoformat()

    sql = f"""
        INSERT INTO integration_errors(datetime_register, error)
            VALUES('{datetime_register}', %(error)s)
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, error)
        conn.commit()
        cur.close()
    except Exception as error:
        print(error)


def insert_product_relation(product_table, product_id, line_id, conn):

    inserted = None

    if 'point' in product_table:
        column = 'point_id'
    elif 'sector' in product_table:
        column = 'sector_id'
    else:
        column = 'company_id'

    sql = f"""
        INSERT INTO {product_table} (product_id, {column})
            VALUES ({product_id}, {line_id})
    """ 

    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        inserted = cur.rowcount
        cur.close()

    except Exception as error:
        print(error)
            



def create_product(data, conn):
    """
    Create the product, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Product.
    """

    inserted = False


    try:
        cur = conn.cursor()
        sql = (
            "INSERT INTO product ("
            "name, "
            "un, "
            "id_capture) "
            "VALUES "
            "('{}','{}', '{}') "
                .format(   
                data['product'],
                'ton',
                data['capture_id'],
            )
        )
        cur.execute(sql)
        conn.commit()
        cur.close()

        rows = cur.rowcount

        if rows:
            inserted = True


        logger.info("Product inserted in PostgreSql.")

    except Exception as error:

        logger.error("Product not inserted: {}".format(data['product']))


    return inserted


def check_filter(date, conn):


    row = 0

    try:
    

        sql = f"""
            SELECT datetime_read
            FROM filter_processes
            WHERE datetime_read = '{date}' 
        """

        cur = conn.cursor()
        cur.execute(sql)
        row = cur.rowcount
        cur.close()

    except Exception as error:
        print(error)

    finally:
        return row


def insert_filter(date, conn):


    inserted = 0

    try:

        sql = f"""
            INSERT INTO filter_processes(datetime_read)
            VALUES ('{date}') 
        """

        cur = conn.cursor()
        cur.execute(sql)
        inserted = cur.rowcount
        conn.commit()
        cur.close()

    except Exception as error:
        print(error)

    finally:
        return inserted
    


def insert_utility(data, conn):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """
    try:
        sql = """
        INSERT INTO public.utility(
            datetime_register, datetime_read, value, plant_equipment_id)
            VALUES ( %(datetime_register)s, %(datetime_read)s, %(value)s, %(plant_equipment_id)s)
        ON CONFLICT ON CONSTRAINT utility_datetime_read_plant_equipment_id_8001152b_uniq
        DO
            UPDATE SET datetime_register=%(datetime_register)s, value=%(value)s
        """
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    finally:
        return inserted


def insert_processes(data, conn):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """
    try:
        sql = """
        INSERT INTO public.processes(
            datetime_register, datetime_read, p_value, plant_equipment_id)
            VALUES (%(datetime_register)s, %(datetime_read)s, %(p_value)s, %(plant_equipment_id)s)
        ON CONFLICT ON CONSTRAINT processes_datetime_read_plant_equipment_id_306ee379_uniq
        DO
            UPDATE SET datetime_register=%(datetime_register)s, p_value=%(p_value)s
        """
        cur = conn.cursor()
        cur.executemany(sql,data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    finally:
        return inserted


def insert_measurement(data, conn):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """

    try:
        sql = """
        INSERT INTO public.measurement(
        datetime_register,
        datetime_read, 
        value_active, 
        value_reactive, 
        period, 
        tension_phase_neutral_a, 
        tension_phase_neutral_b, 
        tension_phase_neutral_c, 
        current_a, 
        current_b, 
        current_c, 
        thd_tension_a, 
        thd_tension_b, 
        thd_tension_c, 
        thd_current_a, 
        thd_current_b, 
        thd_current_c, 
        consolidation_count, 
        plant_equipment_id)
        VALUES (
            %(datetime_register)s,
            %(datetime_read)s, 
            %(value_active)s, 
            %(value_reactive)s, 
            %(period)s, 
            %(tension_phase_neutral_a)s, 
            %(tension_phase_neutral_b)s, 
            %(tension_phase_neutral_c)s, 
            %(current_a)s, 
            %(current_b)s, 
            %(current_c)s, 
            %(thd_tension_a)s, 
            %(thd_tension_b)s, 
            %(thd_tension_c)s, 
            %(thd_current_a)s, 
            %(thd_current_b)s, 
            %(thd_current_c)s, 
            1, 
            %(plant_equipment_id)s
        )
        ON CONFLICT ON CONSTRAINT measurement_datetime_read_plant_equipment_id_22c8604f_uniq
        DO
            UPDATE SET datetime_register=%(datetime_register)s, value_active=%(value_active)s, value_reactive=%(value_reactive)s
        """
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    finally:
        return inserted


def insert_processes_filters(data, conn):

    cols = get_table_columns(conn)

    sql = ''

    for d in data:
        capture_id = d['capture_id']
        column = cols['processes'][capture_id]['column']

        sql += f"""
            INSERT INTO public.filter_processes(
                datetime_read, {column})
                VALUES ('{d['datetime_read']}', '{d['f_value']}')
            ON CONFLICT ON CONSTRAINT filter_processes_pkey
            DO
                UPDATE SET {column} = '{d['f_value']}';
        """


    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        return inserted



def insert_utility_filters(data, conn):

    cols = get_table_columns(conn)

    sql = ''

    for d in data:
        capture_id = d['capture_id']
        column = cols['utility'][capture_id]['column']

        sql += f"""
            INSERT INTO public.filter_utility(
                datetime_read, {column})
                VALUES ('{d['datetime_read']}', '{d['f_value']}')
            ON CONFLICT ON CONSTRAINT filter_utility_pkey
            DO
                UPDATE SET {column} = '{d['f_value']}';
        """


    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        return inserted



def insert_measurement_filters(data, conn):

    cols = get_table_columns(conn)

    sql = ''

    for d in data:
        capture_id = d['capture_id']
        column = cols['measurement'][capture_id]['column']

        sql += f"""
            INSERT INTO public.filter_measurement(
                datetime_read, {column})
                VALUES ('{d['datetime_read']}', '{d['f_value']}')
            ON CONFLICT ON CONSTRAINT filter_measurement_pkey
            DO
                UPDATE SET {column} = '{d['f_value']}';
        """


    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        return inserted


def insert_production(data, conn):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param product: Product of the data.
    @return: Success or fail in the insertion.
    """
    try:
        sql = """
        INSERT INTO public.manufactured(
            datetime_read, value, product_id)
            VALUES (%(datetime_read)s, %(value)s, %(product)s)
        ON CONFLICT ON CONSTRAINT manufactured_datetime_read_product_id_59434b9e_uniq
        DO 
            UPDATE SET value=%(value)s
        """
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))
    finally:
        return inserted


