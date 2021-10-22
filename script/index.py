from datetime import datetime
from jsonschema import validate, ValidationError
from jsonschema.validators import validator_for
import logging
import json
import os
import psycopg2
import re

# Logger settings CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

failed_data = []
index = 0


def lambda_handler(event, context):
    """
    Function that's convert the json data receive into a python dictionary
    @param json_data: json received from client
    """

    try:

        conn = connect_postgres(0)
        result = table_type(conn)

        if result:

            for e in event:
                if 'value_active' in e:
                    data = measurement_to_utility(e)

                    for d in data:
                        data_handler(d)

                
                else:
                    data_handler(e)

        else:
            for e in event:
                data_handler(e)


        if len(failed_data) != 0:
            return {
            'statusCode': 400,
            'body':json.dumps(failed_data, indent=2)
            }

    except Exception as e:
        logging.error(e)



def data_handler(event):
    """
    Initial proccess.
    @param event: Message received.
    """

    print(event)

    global index
    
    event['index'] = index

    index += 1

    SECRET_KEY = "EHu2wf3M0!qA9NEJmUQBpdG^34Z06"

    if 'token' in event.keys(): 
        if event ["token"] != SECRET_KEY :
            return {
            'statusCode': 403,
            'body': json.dumps('Not Authorized')
        }

    logger.info("Received event: " + json.dumps(event, indent=2))

    if data_validate(event):

        data = get_data(event)

        conn = connect_postgres(data)
        if conn:
            if 'value_active' in data:
                register_measurement(conn, data)

            elif 'product' in data:
                register_production(conn, data)

            elif 'p_value' in data:
                register_processes(conn, data)

            elif 'f_value' in data:
                register_filter_table(conn, data)

            else:
                register_utility(conn, data)


        logger.info("End Function")

        return {
            'statusCode': 200,
            'body':json.dumps('ok')
        }



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

    conn = connect_postgres(0)

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



def data_validate(event):

    capture_ids = get_tables_capture_id()

    if 'capture_id' in event and event['capture_id'] in capture_ids:
        return static_validate(event)  
    elif 'value_active' in event or 'value_reactive' in event:
        return measurement_validate(event)
    elif 'product' in event or len(event) == 5:
        return production_validate(event)
    elif 'p_value' in event:
        return processes_validate(event)
    else:
        return utility_validate(event)


def get_col_type():

    cols = get_table_columns()

    col_types = {}

    for c in cols:
        for i in cols[c]:
            col_types[i] = cols[c][i]['type']

    return col_types


def get_table(capture_id):

    cols = get_table_columns()

    for c in cols:
        if capture_id in cols[c].keys():
            return c

    return ''


def static_validate(event):

    errors = {}

    col_type = get_col_type()

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

        failed_data.append(errors)

        return False

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

        failed_data.append(errors)

        return False

    return True

def get_table_columns():

    conn = None

    sql = """
        SELECT 
            value
        FROM
            system_config
        WHERE
            key = 'staticTable'
    """

    try:
        conn = connect_postgres(0)

        cur = conn.cursor()

        cur.execute(sql)

        data = cur.fetchone()

        data = json.loads(data[0])

        cols = {}
        

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
            if conn is not None:
                conn.close()
                return cols

def get_tables_capture_id():

    cols = get_table_columns()

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

    if 'value' in event and type(event['value']) is not float:
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

        failed_data.append(errors)

        return False

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
            regex = re.search(r'^([\d]+)_([\d]+)_([\d]+)$', capture_id)
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

    if 'value' in event and type(event['value']) is not float:
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

        failed_data.append(errors)

        return False

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
        if type(value) is not float:
            errors[v] = f"'{value}' não é do tipo 'number'"

    if invalid_keys: 
        for i in invalid_keys:
            errors[i] = 'Chave fora do padrão'

    if forgotten_keys:
        for f in forgotten_keys:
            errors[f] = 'Chave não encontrada'
    
    if errors:
        errors['index'] = event['index']

        failed_data.append(errors)

        return False

    return True



def validate_data(event):
    """
    Validate the received data
    @param event: Message received
    """

    try:
        if 'value_active' in event or 'value_reactive' in event:

            format = {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {
                            "capture_id": {
                            "type": "string"
                            },
                            "datetime_read": {
                            "type": "string",
                            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}$"
                            },
                            "value_active": {
                            "type": "number"
                            },
                            "value_reactive": {
                            "type": "number"
                            },
                            "tension_phase_neutral_a": {
                            "type": "number"
                            },
                            "tension_phase_neutral_b": {
                            "type": "number"
                            },
                            "tension_phase_neutral_c": {
                            "type": "number"
                            },
                            "current_a": {
                            "type": "number"
                            },
                            "current_b": {
                            "type": "number"
                            },
                            "current_c": {
                            "type": "number"
                            },
                            "thd_tension_a": {
                            "type": "number"
                            },
                            "thd_tension_b": {
                            "type": "number"
                            },
                            "thd_tension_c": {
                            "type": "number"
                            },
                            "thd_current_a": {
                            "type": "number"
                            },
                            "thd_current_b": {
                            "type": "number"
                            },
                            "thd_current_c": {
                            "type": "number"
                            }
                        },
                        "required": [
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
                    }
            
            validate(instance=event, schema=format)

            return True

        if 'product' in  event or len(event) == 4:
            format =  { "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {
                            "capture_id": {
                            "type": "string"
                            },
                            "datetime_read": {
                            "type": "string",
                            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}$"
                            },
                            "value": {
                            "type": "number"
                            },
                            "product":{
                                "type":"string"
                            }
                        },
                        "required": [
                            "capture_id",
                            "datetime_read",
                            "value",
                            "product"
                        ]
                    }

            validate(instance=event, schema=format)

            return True
        
        else:
            format =  { "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {
                            "capture_id": {
                            "type": "string"
                            },
                            "datetime_read": {
                            "type": "string",
                            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}$"
                            },
                            "value": {
                            "type": "number"
                            }
                        },
                        "required": [
                            "capture_id",
                            "datetime_read",
                            "value"
                        ]
                    }
            
            validate(instance=event, schema=format)

            return True



    except ValidationError as e:
        event['error'] = str(e).split('Failed')[0]
        event['error'] = event['error'].replace('\n', '')
        failed_data.append(event)
        logging.exception("Exception occurred")
        return False  



def connect_postgres(data):
    """
    Establish connect with PostgreSql
    @param data: Message received
    @return: Connection
    """
    try:
        rds_host = os.environ.get('RDS_HOST')
        rds_username = os.environ.get('RDS_USERNAME')
        rds_user_pwd = os.environ.get('RDS_USER_PWD')
        rds_db_name = os.environ.get('RDS_DATABASE')

        conn_string = "host=%s user=%s password=%s dbname=%s" % (
        rds_host, rds_username, rds_user_pwd, rds_db_name)
        conn = psycopg2.connect(conn_string)
        logger.info("Connected with Postgres.")

    except ConnectionError as error:
        conn = None
        logger.error("Connecting with Postgres: ", error)

    return conn 



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



def get_data(event):
    """
    Extract the data from the message received.
    @param event: Message received.
    @return: Dictionary with datas.
    """
    try:

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
                'thd_current_c': event['thd_current_c']   
            }
        
        elif 'product' in event:
            
            data = {
                'index':event['index'],
                'capture_id':event['capture_id'],
                'datetime_read':event['datetime_read'],
                'value':event['value'],
                'product':event['product']
            }

        elif 'p_value' in event:

            data = {
                'index':event['index'],
                'capture_id': event['capture_id'],
                'datetime_register': datetime.now().isoformat(),
                'datetime_read': event['datetime_read'],
                'p_value': event['p_value']
            }

        elif 'f_value' in event:

            data = {
                'index':event['index'],
                'capture_id': event['capture_id'],
                'datetime_read': event['datetime_read'],
                'f_value': event['f_value']
            }            

        else:
            data = {
                'index':event['index'],
                'capture_id': event['capture_id'],
                'datetime_register': datetime.now().isoformat(),
                'datetime_read': event['datetime_read'],
                'value': event['value']
            }

    except Exception as error:
        data = {}
        logger.error("Converting data: ", error)

    return data



def get_equipment(conn, data):
    """
    Get the equipment, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Equipment.
    """
    cur = conn.cursor()

    try:
        sql = (
            "SELECT plant_equipment_id "
            "FROM plant_equipment "
            "WHERE id_capture = '{}'".format(data['capture_id'])
        )
        cur.execute(sql)
        equipment = cur.fetchone()
        cur.close()
        if equipment is None:
            capture_id = data['capture_id']
            failed_data.append({
                'index':data['index'],
                'capture_id': f"'{capture_id}' medidor não encontrado"
            })

            return None

        equipment = equipment[0]
        logger.info("Equipment obtained in PostgreSql.")

    except Exception as error:
        equipment = None

        logger.error("Equipment not found: {}".format(data['capture_id']))

    return equipment



def get_product(conn, data):
    """
    Get the product, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Product.
    """
    cur = conn.cursor()

    try:
        sql = (
            "SELECT product_id "
            "FROM product "
            "WHERE id_capture = '{}'".format(data['capture_id'])
        )
        cur.execute(sql)
        product = cur.fetchone()
        cur.close()
        if product is None:
            raise Exception
        product = product[0]
        logger.info("Product obtained in PostgreSql.")

    except Exception as error:
        product = None

        logger.error("Product: {} will be created".format(data['product']))

    return product



def create_product(conn, data):
    """
    Create the product, corresponding to the message received.
    @param conn: Connection with PostgreSql
    @param data: Datas received.
    @return: Product.
    """

    cur = conn.cursor()

    inserted = False

    try:
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


def check_filter(date):

    conn = None 

    row = 0

    try:
        conn = connect_postgres(0)

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
        if conn is not None:
            conn.close()
            return row


def insert_filter(date):

    conn = None 

    inserted = 0

    try:
        conn = connect_postgres(0)

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
        if conn is not None:
            conn.close()
            return inserted
    




def insert_utility(conn, data, plant_equipment_id):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """
    try:
        sql = (
            "INSERT INTO utility ("
            "datetime_register, "
            "datetime_read, "
            "value, "
            "plant_equipment_id) "
            "VALUES "
            "('{}','{}', '{}', '{}') "
                .format(   
                data['datetime_register'],
                data['datetime_read'],
                data['value'],
                plant_equipment_id,
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    return inserted


def insert_processes(conn, data, plant_equipment_id):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """
    try:
        sql = (
            "INSERT INTO processes ("
            "datetime_register, "
            "datetime_read, "
            "p_value, "
            "plant_equipment_id) "
            "VALUES "
            "('{}','{}', '{}', '{}') "
                .format(   
                data['datetime_register'],
                data['datetime_read'],
                data['p_value'],
                plant_equipment_id,
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    return inserted


def insert_measurement(conn, data, plant_equipment_id):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param equipment: Equipment of the datas.
    @return: Success or fail in the insertion.
    """
    try:
        sql = (
            "INSERT INTO measurement ("
            "datetime_register, "
            "datetime_read, "
            "value_active, "
            "value_reactive, "
            "period, "
            "tension_phase_neutral_a, "
            "tension_phase_neutral_b, "
            "tension_phase_neutral_c, "
            "current_a, "
            "current_b, "
            "current_c, "
            "thd_tension_a, "
            "thd_tension_b, "
            "thd_tension_c, "
            "thd_current_a,"
            "thd_current_b,"
            "thd_current_c, "
            "plant_equipment_id, "
            "consolidation_count) "
            "VALUES "
            "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}','{}','{}', '{}','{}','{}','{}','{}','{}','{}', '{}') "
                .format(   
                data['datetime_register'],
                data['datetime_read'],
                data['value_active'],
                data['value_reactive'],
                data['period'],
                data['tension_phase_neutral_a'],
                data['tension_phase_neutral_b'],
                data['tension_phase_neutral_c'],
                data['current_a'],
                data['current_b'],
                data['current_c'],
                data['thd_tension_a'],
                data['thd_tension_b'],
                data['thd_tension_c'],
                data['thd_current_a'],
                data['thd_current_b'],
                data['thd_current_c'],
                plant_equipment_id,
                1,
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    return inserted


def insert_processes_filters(conn, data):

    cols = get_table_columns()
    capture_id = data['capture_id']
    column = cols['processes'][capture_id]['column']

    try:
        sql = """
            INSERT INTO public.filter_processes(
                datetime_read, {0})
                VALUES (%(datetime_read)s, %(f_value)s)
            ON CONFLICT ON CONSTRAINT processes_filters_pkey
            DO
                UPDATE SET {0} = %(f_value)s;
        """.format(column)

        conn = connect_postgres(0)
        cur = conn.cursor()
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return inserted



def insert_utility_filters(conn, data):

    cols = get_table_columns()
    capture_id = data['capture_id']
    column = cols['utility'][capture_id]['column']

    try:
        sql = """
            INSERT INTO public.filter_utility(
                datetime_read, {0})
                VALUES (%(datetime_read)s, %(f_value)s)
            ON CONFLICT ON CONSTRAINT utility_filters_pkey
            DO
                UPDATE SET {0} = %(f_value)s;
        """.format(column)

        conn = connect_postgres(0)
        cur = conn.cursor()
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return inserted



def insert_measurement_filters(conn, data):

    cols = get_table_columns()
    capture_id = data['capture_id']
    column = cols['measurement'][capture_id]['column']

    try:
        sql = """
            INSERT INTO public.filter_measurement(
                datetime_read, {0})
                VALUES (%(datetime_read)s, %(f_value)s)
            ON CONFLICT ON CONSTRAINT measurement_filters_pkey
            DO
                UPDATE SET {0} = %(f_value)s;
        """.format(column)

        conn = connect_postgres(0)
        cur = conn.cursor()
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")
        
    except Exception as error:
        inserted = False
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return inserted



def register_utility(conn, data):
    """
    Register utility.
    @param conn: Connection with PostgreSql.
    @param data: Data to be inserted.
    @return: True
    """
    equipment = get_equipment(conn, data)
    if equipment:
        if not update_utility(conn, data, equipment):
            insert_utility(conn, data, equipment)
    return True


def register_processes(conn, data):
    """
    Register utility.
    @param conn: Connection with PostgreSql.
    @param data: Data to be inserted.
    @return: True
    """
    equipment = get_equipment(conn, data)
    if equipment:
        date = data['datetime_read']
        filter = check_filter(date)

        if filter == 0:
            insert_filter(date)
        if not update_processes(conn, data, equipment):
            insert_processes(conn, data, equipment)
    return True


def register_filter_table(conn, data):

    table = get_table(data['capture_id'])

    if table == 'processes':
        insert_processes_filters(conn, data)
    elif table == 'utility':
        insert_utility_filters(conn, data)
    elif table == 'measurement':
        insert_measurement_filters(conn, data)    
    else:
         logger.info('Table not found')


def register_measurement(conn, data):
    """
    Register measurement.
    @param conn: Connection with PostgreSql.
    @param data: Data to be inserted.
    @return: True
    """
    equipment = get_equipment(conn, data)
    if equipment:
        if not update_measurement(conn, data, equipment):
            insert_measurement(conn, data, equipment)
    return True



def update_utility(conn, data, equipment):
    """
    Update utility into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be updated.
    @param equipment: Equipment of the Datas.
    @return: Success or failure in the update.
    """
    sql = ''
    try:
        sql = (
            "UPDATE "
            "utility "
            "SET "
            "datetime_register = '{}', "
            "value = '{}' "
            "WHERE "
            "plant_equipment_id = {} AND "
            "datetime_read = '{}';".format(   
                data['datetime_register'],
                data['value'],
                equipment,
                data['datetime_read']
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        if updated:
            logger.info("Updated in PostgreSql - {}".format(updated))

    except Exception as error:
        updated = None
        logger.error("Updating in PostgreSql: {}, SQL: {}".format(error, sql))

    return updated   


def update_processes(conn, data, equipment):
    """
    Update processes into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be updated.
    @param equipment: Equipment of the Datas.
    @return: Success or failure in the update.
    """
    sql = ''
    try:
        sql = (
            "UPDATE "
            "processes "
            "SET "
            "datetime_register = '{}', "
            "p_value = '{}' "
            "WHERE "
            "plant_equipment_id = {} AND "
            "datetime_read = '{}';".format(   
                data['datetime_register'],
                data['p_value'],
                equipment,
                data['datetime_read']
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        if updated:
            logger.info("Updated in PostgreSql - {}".format(updated))

    except Exception as error:
        updated = None
        logger.error("Updating in PostgreSql: {}, SQL: {}".format(error, sql))

    return updated   


def update_measurement(conn, data, equipment):
    """
    Update measurement into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be updated.
    @param equipment: Equipment of the Datas.
    @return: Success or failure in the update.
    """
    sql = ''
    try:
        sql = (
            "UPDATE "
            "measurement "
            "SET "
            "datetime_register = '{}', "
            "value_active = {}, "
            "value_reactive = {}, "
            "period = {}, "
            "tension_phase_neutral_a = (tension_phase_neutral_a+{})/(consolidation_count+1), "
            "tension_phase_neutral_b = (tension_phase_neutral_b+{})/(consolidation_count+1), "
            "tension_phase_neutral_c = (tension_phase_neutral_c+{})/(consolidation_count+1), "
            "current_a = (current_a+{})/(consolidation_count+1), "
            "current_b = (current_b+{})/(consolidation_count+1), "
            "current_c = (current_c+{})/(consolidation_count+1), "
            "thd_tension_a = {},"
            "thd_tension_b = {},"
            "thd_tension_c = {},"
            "thd_current_a = {},"
            "thd_current_b = {},"
            "thd_current_c = {},"
            "consolidation_count = consolidation_count+1 "
            "WHERE "
            "plant_equipment_id = {} AND "
            "datetime_read = '{}';".format(  
                data['datetime_register'],
                data['value_active'],
                data['value_reactive'],
                data['period'],
                data['tension_phase_neutral_a'],
                data['tension_phase_neutral_b'],
                data['tension_phase_neutral_c'],
                data['current_a'],
                data['current_b'],
                data['current_c'],
                data['thd_tension_a'],
                data['thd_tension_b'],
                data['thd_tension_c'],
                data['thd_current_a'],
                data['thd_current_b'],
                data['thd_current_c'],
                equipment,
                data['datetime_read']
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        if updated:
            logger.info("Updated in PostgreSql - {}".format(updated))

    except Exception as error:
        updated = None
        logger.error("Updating in PostgreSql: {}, SQL: {}".format(error, sql))

    return updated



def register_production(conn, data):
    """
    Register production.
    @param conn: Connection with PostgreSql.
    @param data: Data to be inserted.
    @return: True
    """
    product = get_product(conn, data)
    if not product:
        if create_product(conn, data):
            product = get_product(conn, data)
            if not update_production(conn, data, product):
                insert_production(conn, data, product)
    else:
        if not update_production(conn, data, product):
            insert_production(conn, data, product)
    return True



def update_production(conn, data, product):
    """
    Update production into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be updated.
    @param equipment: Equipment of the Datas.
    @return: Success or failure in the update.
    """
    sql = ''
    try:
        sql = (
            "UPDATE "
            "manufactured "
            "SET "
            "value = '{}' "
            "WHERE "
            "product_id = {} AND "
            "datetime_read = '{}';".format(   
                data['value'],
                product,
                data['datetime_read']
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        if updated:
            logger.info("Updated in PostgreSql - {}".format(updated))

    except Exception as error:
        updated = None
        logger.error("Updating in PostgreSql: {}, SQL: {}".format(error, sql))

    return updated   



def insert_production(conn, data, product):
    """
    Insert datas into PostgreSql.
    @param conn: Connection with PostgreSql
    @param data: Data to be inserted
    @param product: Product of the data.
    @return: Success or fail in the insertion.
    """
    try:
        sql = (
            "INSERT INTO manufactured ("
            "datetime_read, "
            "value, "
            "product_id) "
            "VALUES "
            "('{}','{}', '{}') "
                .format(   
                data['datetime_read'],
                data['value'],
                product,
            )
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        inserted = True
        logger.info("Inserted in PostgreSql.")

    except Exception as error:
        inserted = False
        logger.error("Inserting in PostgreSql: {}, SQL: {}".format(error, sql))

    return inserted

    