'''
    Executes commands
'''

import json
import os
import shutil
import signal
import socket
from sqlite3 import Date
import sys
from datetime import datetime
import dateutil.parser
from functools import reduce
import pandas as pd
import copy

import pymongo

from type_def import Types

PORT = 43569
HOST = "localhost"
BUFF_SIZE = 102400


def exit_handler(_1, _2):
    sys.exit(0)


signal.signal(signal.SIGINT, exit_handler)


class Server:

    def __init__(self, password: str = None):
        self.current_db = None

        if password is None:
            password = os.getenv('MONGO_PWD', default=None)

        if password is None:
            print("Error: no password for MongoDB")
            exit(1)

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((HOST, PORT))
        self.client_s = None

        self.client = pymongo.MongoClient(
            f"mongodb+srv://abkr:{password}@abkr.wfrnm.mongodb.net/ABKR?retryWrites=true&w=majority"
        )
        self.db = None
        self.send_done = True

    def run(self):
        self.s.listen()

        while True:
            self.send_done = True
            self.client_s, _ = self.s.accept()
            command_list = self.client_s.recv(BUFF_SIZE).decode().split(' ')
            check_for_types = True

            for i, val in enumerate(command_list):
                try:
                    num = int(val)
                except Exception:
                    continue
                if check_for_types:
                    command_list[i] = Types(num)
                if command_list[i] in (Types.VALUES, Types.FROM):
                    check_for_types = False

            self.__run(command_list)

            if self.send_done:
                self.__send_msg("done")
            self.client_s.close()

    def __send_msg(self, string: str):
        if self.client_s is not None:
            self.client_s.send(string.encode())

    def __run(self, command_list: list):
        '''
            Executes the commands
        '''

        # create
        if command_list[0] == Types.CREATE:
            # create database
            if command_list[1] == Types.DATABASE:
                self.__create_database(command_list)

            # create table
            elif command_list[1] == Types.TABLE:
                self.__create_table(command_list)

        # drop
        elif command_list[0] == Types.DROP:
            # drop database
            if command_list[1] == Types.DATABASE:
                self.__drop_database(command_list)

            # drop table
            elif command_list[1] == Types.TABLE:
                self.__drop_table(command_list)

        # use database
        elif command_list[0] == Types.USE:
            self.__use_database(command_list)

        # insert
        elif command_list[0] == Types.INSERT:
            self.__insert(command_list)

        # delete
        elif command_list[0] == Types.DELETE:
            self.__delete(command_list)

        # add
        elif command_list[0] == Types.ADD:
            # add primary key
            if command_list[1] == Types.PK:
                self.__add_primary_key(command_list)

            # add foreign key
            elif command_list[1] == Types.FK:
                self.__add_foreign_key(command_list)

            # add unique
            elif command_list[1] == Types.UQ:
                self.__add_unique_key(command_list)

            # add index
            elif command_list[1] == Types.INDEX:
                self.__add_index(command_list)

        # select
        elif command_list[0] == Types.SELECT:
            self.__select(command_list)

    
    # select

    def __select(self, command_list):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return
        
        table, columns_from, conditions, columns_where, join_conditions, has_join = self.__parse_select_command(command_list)
        if table == 0 or columns_from == 0:
            return
        if not has_join:
            data = self.__select_from_one_table(table, conditions, columns_where)
            answer = self.__format_into_table_selected_columns(data, table, columns_from)
            self.__send_msg(answer)
            self.send_done = True
        else:
            join_data = {}
            self.__join_tables(table, columns_where, conditions, join_conditions, columns_from)
        

    def __join_tables(self, table, columns_where, conditions, join_conditions, columns_from):
        foreign_key_condition_exists_between_columns = self.__check_foreign_key_restraint_on_join_conditions(table, join_conditions)
        if not foreign_key_condition_exists_between_columns:
            return
        print(columns_where, conditions, join_conditions, columns_from)
        join_data = {}
        for column in columns_where:
            table_abreviation, column = column.split(".")
            table_of_column = table[table_abreviation]
            print(table_of_column, column)
    

    def __check_foreign_key_restraint_on_join_conditions(self, table, join_conditions):
        for condition in join_conditions:
            print(condition)
            table_abreviation, column1 = condition[0].split(".")
            table1 = table[table_abreviation]
            table_abreviation, column2 = condition[1].split(".")
            table2 = table[table_abreviation]
            path1 = self.current_db + "/" + table1 + ".json"
            with open(path1, "r", encoding="utf-8") as f:
                data = json.load(f)
            for fk in data[0]["foreign_keys"]:
                if fk["key"][0] == column1 and fk["table"] == table2 and fk["column"][0] == column2:
                        return True
            for fk in data[0]["child_tables"]:
                if fk["key"][0] == column1 and fk["table"] == table2 and fk["column"][0] == column2:
                        return True
        self.__send_msg("There is no foreign key relationship between " + table1 + " - " + column1 + " and " + table2 + " - " + column2)
        self.send_done = False
        return False

    
    # def __check_foreign_key_match 
    
    def __select_from_one_table(self, table, conditions, columns_where):
        if columns_where != []:
            if not self.__correct_conditions_for_unindexed_columns(table, columns_where, conditions):
                return
            has_index, pk_is_selected, pk_name = self.__has_index(table, columns_where)
            unindexed_columns = list(copy.deepcopy(set(columns_where)))
            for element in unindexed_columns:
                if element in has_index:
                    unindexed_columns.remove(element)
            if len(has_index) > 0:
                ids_from_indexed_columns = []
                for col in has_index:
                    ids_from_indexed_columns.append(self.__get_ids_from_indexed_table(table, col, conditions[col], pk_is_selected, pk_name))
                if len(ids_from_indexed_columns) > 1:
                    ids_from_indexed_columns = reduce(lambda x, y: x+y, ids_from_indexed_columns)
                    s = pd.Series(ids_from_indexed_columns)
                    ids_from_indexed_columns = s[s.duplicated()].unique().tolist()
                else:
                    ids_from_indexed_columns = reduce(lambda x, y: x+y, ids_from_indexed_columns)
                if len(ids_from_indexed_columns) == 0:
                    # answer = "TABLE " + str(len(columns_from)) + " ".join(columns_from)
                    # self.__send_msg(answer)
                    return []
                elif len(unindexed_columns) == 0:
                    data = self.db[table].find({'_id': { '$in' : ids_from_indexed_columns}})
                else:
                    data = self.__get_data_from_unindexed_columns(table, unindexed_columns, conditions, ids_from_indexed_columns)
            else:
                data = self.__get_data_from_unindexed_columns(table, unindexed_columns, conditions, []) 
        else:
            data = self.db[table].find()
        return data

    def __get_data_from_unindexed_columns(self, table, unindexed_columns, conditions, ids_from_indexed_columns):
        all_column_names = self.__get_column_names(table)
        indexes = []
        types = []
        cols = []
        for i, column_name in enumerate(all_column_names):
            if column_name in unindexed_columns:
                indexes.append(i)
                types.append(self.__get_column_type(table, column_name))
                cols.append(column_name)
        return self.__get_list_of_values_by_condition(table, indexes, conditions, types, cols, ids_from_indexed_columns)

    def __get_list_of_values_by_condition(self, table, indexes, conditions, types, unindexed_column_names, ids_from_indexed_columns):
        collection = self.db[table]
        matching_rows = []
        if ids_from_indexed_columns == []:
            values = collection.find()
        else:
            values = collection.find({'_id': { '$in': ids_from_indexed_columns }})
        for val in values:
            data = val["Value"].split("#")
            column_ok = True
            for i, ind in enumerate(indexes):
                if not column_ok:
                    break
                for cond in conditions[unindexed_column_names[i]]:
                    if not column_ok:
                        break
                    condition_value = self.__change_type(cond[1], cond[2])
                    operator = cond[0]
                    column_value = self.__change_type(data[ind - 1], cond[2])
                    match operator:
                        # case Types.EQ:
                        case '21':
                            if condition_value != column_value:
                                column_ok = False
                        # case Types.NE:
                        case "26":
                            if condition_value == column_value:
                                column_ok = False
                        # case Types.LT:
                        case "22":
                            if condition_value <= column_value:
                                column_ok = False
                        # case Types.GT:
                        case "23":
                            if condition_value >= column_value:
                                column_ok = False
                        # case Types.GE:
                        case "25":
                            if condition_value > column_value:
                                column_ok = False
                        # case Types.LE:
                        case "24":
                            if condition_value < column_value:
                                column_ok = False
            if column_ok:
                matching_rows.append(val)
        return matching_rows
    
    def __get_ids_from_indexed_table(self, table, column, conditions, pk_is_selected, pk_name):
        column_ids = []
        if pk_is_selected and pk_name == column:
            index_table_name = table
        else:
            index_table_name = "index_" + table + "_" + column
        for cond in conditions:
            operator, value, type = cond
            value = self.__change_type(value, type)
            match operator:
                # case Types.EQ:
                case '21':
                    values_object = self.db[index_table_name].find({'_id': value})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append([val['_id']])
                            else:
                                column_ids.append(val["Value"])
                # case Types.NE:
                case "26":
                    values_object = self.db[index_table_name].find({ '_id': { "$ne" : value }})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append(val['_id'])
                            else:
                                column_ids.append(val["Value"])
                # case Types.LT:
                case "22":
                    values_object = self.db[index_table_name].find({ '_id': { "$lt" : value }})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append(val['_id'])
                            else:
                                column_ids.append(val["Value"])
                # case Types.GT:
                case "23":
                    values_object = self.db[index_table_name].find({ '_id': { "$gt" : value }})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append(val['_id'])
                            else:
                                column_ids.append(val["Value"])
                # case Types.GE:
                case "25":
                    values_object = self.db[index_table_name].find({ '_id': { "$gte" : value }})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append(val['_id'])
                            else:
                                column_ids.append(val["Value"])
                # case Types.LE:
                case "24":
                    values_object = self.db[index_table_name].find({ '_id': { "$lte" : value }})
                    if values_object != None:
                        for val in values_object:
                            if pk_is_selected and pk_name == column:
                                column_ids.append(val['_id'])
                            else:
                                column_ids.append(val["Value"])
            if len(column_ids) != 0:
                if pk_name != column:
                    column_ids = reduce(lambda x, y: x+y, column_ids)
        if len(conditions) == 1:
            return column_ids
        else:
            s = pd.Series(column_ids)
            column_ids =  s[s.duplicated()].unique().tolist()
        return column_ids

    def __correct_conditions_for_unindexed_columns(self, table, columns, conditions):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for col in data[1:]:
            column_name = col['column_name']
            if column_name in columns:
                match col['type']:
                    case 'int':
                        for cond in conditions[column_name]:
                            if not self.__checkInt(cond[1]):
                                self.__send_msg("The condition for the " + column_name + " column needs to be of type int")
                                self.send_done = False
                                return False
                    case 'float':
                        for cond in conditions[column_name]:
                            if not self.__checkFloat(cond[1]):
                                self.__send_msg("The condition for the " + column_name + " column needs to be of type float")
                                self.send_done = False
                                return False
                    case 'bit':
                        for cond in conditions[column_name]:
                            if not self.__checkBit(cond[1]):
                                self.__send_msg("The condition for the " + column_name + " column needs to be of type bool")
                                self.send_done = False
                                return False
                    case 'date':
                        for cond in conditions[column_name]:
                            if not self.__checkDate(cond[1]):
                                self.__send_msg("The condition for the " + column_name + " column needs to be of type date")
                                self.send_done = False
                                return False
                    case 'datetime':
                       for cond in conditions[column_name]:
                            if not self.__checkDateTime(cond[1]):
                                self.__send_msg("The condition for the " + column_name + " column needs to be of type datetime")
                                self.send_done = False
                                return False  
        return True    

    def __parse_select_command(self, command_list):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        from_index = command_list.index(Types.FROM)
        table = command_list[from_index + 1]
        if not self.__table_exists(table):
            self.__send_msg("The " + table + " table doesn't exist")
            self.send_done = False
            return 0, 0, 0, 0, 0, 0
        
        if command_list[1] == Types.ALL or command_list[1] == "*":
            columns_select = self.__get_column_names(table)
        elif command_list[1] == Types.COLUMNS:
            columns_select = [command_list[i] for i in range(2,from_index)]

        has_join = 'join' in command_list

        if has_join:
            tables = {
                command_list[from_index + 2] : command_list[from_index + 1]
            }
            join_positions = [ i for i in range(len(command_list)) if command_list[i] == 'join' ]
            for poz in join_positions:
                tables[command_list[poz + 2]] = command_list[poz + 1]
            join_conditions = []
            for join_poz in join_positions:
                join_conditions.append([command_list[join_poz + 4], command_list[join_poz + 6]])

        for column in columns_select:
            if has_join:
                table_abreviation, col = column.split(".")
                if not self.__column_exists(tables[table_abreviation], col):
                    self.__send_msg("The " + col + " column doesn't exist int the " + tables[table_abreviation] + " table")
                    self.send_done = False
                    return 0, 0, 0, 0, 0, 0
            elif not self.__column_exists(table, column):
                self.__send_msg("The " + column + " column doesn't exist")
                self.send_done = False
                return 0, 0, 0, 0, 0, 0
        if has_join and len(command_list) == join_positions[-1] + 3 or len(command_list) == from_index + 2:
            return table, columns_select, [], [], [], has_join
        
        if not has_join:
            conditions = command_list[from_index + 3:]
            del conditions[3::4]
            columns_where = conditions[0::3]
        else:
            where_index = command_list.index('17')
            conditions = command_list[where_index + 1:]
            del conditions[3::4]
            columns_where = conditions[0::3]
        
        for column in columns_where:
            if has_join:
                table_abreviation, col = column.split(".")
                if not self.__column_exists(tables[table_abreviation], col):
                    self.__send_msg("The " + col + " column doesn't exist int the " + tables[table_abreviation] + " table")
                    self.send_done = False
                    return 0, 0, 0, 0, 0, 0 
            elif not self.__column_exists(table, column):
                self.__send_msg("The " + column + " column doesn't exist")
                self.send_done = False
                return 0, 0, 0, 0, 0, 0

        cond_dict = {}
        for i in range(0, len(conditions), 3):
            operator = conditions[i+1]
            value = conditions[i+2]
            if has_join:
                table_abreviation, column = conditions[i].split(".")
                column_type = self.__get_column_type(tables[table_abreviation], column)
                column = conditions[i]
            else:
                column = conditions[i]
                column_type = self.__get_column_type(table, column)
            if column in cond_dict:
                cond_dict[column].append([operator, value, column_type])
            else:
                cond_dict[column] = [[operator, value, column_type]]

        if not has_join:
            return table, columns_select, cond_dict, columns_where, [], has_join
        else:
            return tables, columns_select, cond_dict, columns_where, join_conditions, has_join

    def __has_index(self, table, columns):
        has_index = []
        column_names = self.__get_column_names(table)
        pk_name = column_names[0]
        pk_is_selected = column_names[0] in columns
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for column in data[1:]:
            column_name = column['column_name']
            if column_name in columns and column['index'] == "true":
                has_index.append(column_name)
        return has_index, pk_is_selected, pk_name

    def __format_into_table_selected_columns(self, cursor, table_name, columns_select):
        f = open('select.txt', 'w')
        column_names = self.__get_column_names(table_name)
        answer = "TABLE " + str(len(columns_select))
        f.write(" ".join(columns_select))
        id = column_names[0] in columns_select
        indexes = []
        for i, col in enumerate(column_names[1:]):
            if col in columns_select:
                indexes.append(i)
        for cur in cursor:
            print(cur)
            f.write("\n")
            if id:
                f.write(str(cur["_id"]) + " ")
            values = cur["Value"].split("#")
            for i in indexes:
                f.write(values[i] + " ")
        return answer

    # cerate database, table
    def __create_database(self, command_list):
        if self.__database_exists(command_list[2]):
            self.__send_msg("Database already exists")
            self.send_done = False
            return

        os.mkdir(command_list[2])

        # the database only gets created if we add a collection to it
        db = self.client[command_list[2]]
        db.create_collection("database created")

    def __create_table(self, command_list):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if self.__table_exists(command_list[2]):
            self.__send_msg("Table already exists")
            self.send_done = False
            return

        path = self.current_db + '/' + command_list[2] + '.json'

        data = [{
            "column_name": "keys",
            "primary_keys": [],
            "foreign_keys": [],
            "child_tables": []
        }]
        primary_key = {
            "column_name": command_list[4],
            "type": command_list[3].name.lower(),
            "index": "true",
            "unique": "true",
            "primary_key": "true",
            "foreign_key": "false",
            "parent_table": "false",
        }
        data.append(primary_key)
        data[0]["primary_keys"].append([command_list[4], 1])
        i = 5
        while i < len(command_list):
            temp_data = {
                "column_name": command_list[i + 1],
                "type": command_list[i].name.lower(),
                "index": "false",
                "unique": "false",
                "primary_key": "false",
                "foreign_key": "false",
                "parent_table": "false",
            }
            data.append(temp_data)
            i += 2

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        db = self.client[self.current_db]
        db.create_collection(command_list[2])

        self.__delete_pinning_collection()

    # drop database, table

    def __drop_database(self, command_list):
        if not self.__database_exists(command_list[2]):
            self.__send_msg("Database doesn't exist")
            self.send_done = False
            return

        shutil.rmtree(command_list[2])

        db = self.client[command_list[2]]
        collections = db.list_collection_names()
        for col in collections:
            db.drop_collection(col)
        self.current_db = None

    def __drop_table(self, command_list):
        table = command_list[2]
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if not self.__table_exists(table):
            self.__send_msg("Table doesn't exist")
            self.send_done = False
            return

        path = self.current_db + '/' + table + '.json'
        db = self.client[self.current_db]
        db.drop_collection(table)

        # if there's only 1 table and we delete it, the db will dissapear
        # we insert a pinning table for it to remain existing
        collections = db.list_collection_names()
        if not collections:
            db.create_collection("database created")

        # deleting index tables belonging to the deleted table
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for column in range(2, len(data)):
            if data[column]["index"] == "true":
                column_name = data[column]["column_name"]
                index_table_name = "index_" + str(table) + "_"\
                    + str(column_name)
                db.drop_collection(index_table_name)

        # remove json
        os.remove(path)

    # deleting from the database, functions checking the correctness of it
    
    def __delete(self, command_list):
        del command_list[3]
        del command_list[1]
        table = command_list[1]

        id_column_name = self.__get_id_column_name(table)
        id_type = self.__get_column_type(table, id_column_name)
        id = self.__change_type(command_list[2], id_type)
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if not self.__table_exists(table):
            self.__send_msg("Table doesn't exist")
            self.send_done = False
            return

        if not self.__is_id_in_table(table, id):
            self.__send_msg("the \"" + str(id) + "\" ID doesn't exist in the "
                            + table + " table")
            self.send_done = False
            return

        if not self.__exists_external_reference_from_foreign_keys(table, id):
            return

        values_object = self.db[table].find_one({'_id': id})
        values = values_object["Value"]
        values = values.split("#")

        self.db[table].delete_one({'_id': id})

        self.__delete_from_index_tables(table, id, values)

    def __exists_external_reference_from_foreign_keys(self, table, id):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        child_tables = data[0]["child_tables"]
        for child_table in child_tables:
            # get values, from fk
            fk_table = child_table["table"]
            fk_column = child_table["column"][0]
            fk_column_index = child_table["column"][1]
            fk_column_type = child_table["column"][2]
            parent_table_column_index = child_table["key"][1]
            parent_table_column_type = child_table["key"][2]
            # check if parent tables's column has index table
            path2 = self.current_db + '/' + fk_table + '.json'
            data2 = []
            with open(path2, "r", encoding="utf-8") as f:
                data2 = json.load(f)
            has_index = data2[fk_column_index]["index"] == "true"
            # get the values you want to delete
            document = self.db[table].find({"_id": id})
            values = document[0]["Value"]
            values = values.split("#")
            value_from_parent = self.__change_type(values[parent_table_column_index - 2], parent_table_column_type)
            if not has_index:
                values_from_fk = self.__get_list_of_values_by_index(fk_table, [fk_column_index - 2], [fk_column_type])
                if value_from_parent in values_from_fk[fk_column_index - 2]:
                    self.__send_msg("Can't delete row, because the " + str(value_from_parent) + " is present as a foreign key int the \""
                                    + fk_table + "\" table's \"" + fk_column + "\" column")
                    self.send_done = False
                    return False
            else:
                index_table_name = "index_" + str(fk_table) + "_" + str(fk_column)
                value = list(self.db[index_table_name].find({"_id": value_from_parent}))
                if len(value) == 0:
                    error_msg = "foreign key error: the \"" + str(value_from_parent) + "\" foreign key doesn't exist in the original table (" + str(fk_table) + " - " + str(fk_column) + ")"
                    self.__send_msg(error_msg)
                    self.send_done = False
                    return False
            return True
        return True

    def __delete_from_index_tables(self, table, id, values):

        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        index_true_column_pozition = []
        index_true_column_name = []
        index_true_column_type = []

        for column in range(2, len(data)):
            if data[column]["index"] == "true":
                index_true_column_pozition.append(column - 2)
                index_true_column_name.append(data[column]["column_name"])
                index_true_column_type.append(data[column]["type"])
        for i in range(len(index_true_column_pozition)):
            index_table_name = "index_" + str(table) + "_" + str(index_true_column_name[i])
            index_true_value = self.__change_type(values[index_true_column_pozition[i]], index_true_column_type[i])
            self.db[index_table_name].update_one({"_id": index_true_value}, {'$pull': {"Value": id}})
            # if array of values is empty after deleting the index element, delete the document
            values_object = self.db[index_table_name].find_one({'_id': index_true_value})
            values_index = values_object["Value"]
            id_index = values_object["_id"]
            if not values_index:
                self.db[index_table_name].delete_one({'_id': id_index})

    # set the given database as the current one

    def __use_database(self, command_list):
        if not self.__database_exists(command_list[1]):
            self.__send_msg("Database doesn't exist")
            self.send_done = False
            return

        self.current_db = command_list[1]
        self.db = self.client[command_list[1]]

    # inserting data into the database, functions checking the correctness of it

    def __insert(self, command_list):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if not self.__table_exists(command_list[1]):
            self.__send_msg("Table doesn't exist")
            self.send_done = False
            return

        if not self.__insert_data_is_correct(command_list):
            return

        if not self.__insert_data_check_unique(command_list):
            return

        if not self.__insert_data_check_foreign_key(command_list):
            return

        data_list = command_list[2].split("#")
        id = data_list[0]

        # get id column name
        id_column_name = self.__get_id_column_name(command_list[1])
        index = len(id) + 1
        pk_type = self.__get_column_type(command_list[1], id_column_name)
        id = self.__change_type(id, pk_type)
        values = command_list[2][index:]
        self.db[command_list[1]].insert_one({"_id": id, "Value": values})
        
        self.__insert_into_index_tables(command_list[1], id, data_list)
        self.send_done = True

    def __insert_into_index_tables(self, table, id, values):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        index_true = []
        index_true_column_name = []
        index_true_column_type = []

        for column in range(2, len(data)):
            if data[column]["index"] == "true":
                index_true.append(column - 1)
                index_true_column_name.append(data[column]["column_name"])
                index_true_column_type.append(data[column]["type"])
        for i in range(0, len(index_true)):
            index_table_name = "index_" + str(table) + "_" + index_true_column_name[i]
            column_value = self.__change_type(values[index_true[i]], index_true_column_type[i])
            if self.db[index_table_name].count_documents({"_id": column_value}) > 0:
                self.db[index_table_name].update_one({"_id": column_value}, {'$push': {"Value": id}})
            else:
                self.db[index_table_name].insert_one({"_id": column_value, "Value": [id]})

    def __insert_data_check_foreign_key(self, command_list):
        data_list = command_list[2].split("#")
        data_list.remove(data_list[0])
        path = self.current_db + '/' + command_list[1] + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        list_of_fks = data[0]["foreign_keys"]
        for fks in list_of_fks:
            table2 = fks["table"]
            insert_data_index = fks["key"][1] - 2
            insert_data_type = fks["key"][2]
            column2_index = [fks["column"][1] - 2]
            column2_name = fks["column"][0]
            column2_type = [fks["column"][2]]
            # check if parent tables's column has index table
            path2 = self.current_db + '/' + table2 + '.json'
            data2 = []
            with open(path2, "r", encoding="utf-8") as f:
                data2 = json.load(f)
            has_index = data2[column2_index[0] + 2]["index"] == "true"

            # if a column has index table, we use it, if not, we iterate through the values
            if not has_index:
                fk_values_in_table2 = self.__get_list_of_values_by_index(table2, column2_index, column2_type)
                if self.__change_type(data_list[insert_data_index], insert_data_type) not in fk_values_in_table2[column2_index[0]]:
                    error_msg = "foreign key error: the \"" + str(data_list[insert_data_index]) + "\" foreign key doesn't exist in the original table (" + str(table2) + " - " + str(column2_name) + ")"
                    self.__send_msg(error_msg)
                    self.send_done = False
                    return False
            else:
                column_names = self.__get_column_names(table2)
                pk_is_selected = column_names[0] in column2_name
                if pk_is_selected:
                    index_table_name = table2
                else:
                    index_table_name = "index_" + str(table2) + "_" + str(column2_name)
                value = list(self.db[index_table_name].find({"_id": self.__change_type(data_list[insert_data_index], insert_data_type)}))
                if len(value) == 0:
                    error_msg = "foreign key error: the \"" + str(data_list[insert_data_index]) + "\" foreign key doesn't exist in the original table (" + str(table2) + " - " + str(column2_name) + ")"
                    self.__send_msg(error_msg)
                    self.send_done = False
                    return False
        return True

    def __insert_data_check_unique(self, command_list):
        data_list = command_list[2].split("#")
        table = command_list[1]
        id_name = self.__get_id_column_name(table)
        id_type = self.__get_column_type(table, id_name)
        id = self.__change_type(data_list[0], id_type)
        # check uniqueness of primary key
        if self.__is_id_in_table(table, id):
            error_msg = "unique error: the \"" + str(id) + "\" id already exists in the table"
            self.__send_msg(error_msg)
            self.send_done = False
            return False
        # check uniquness of other columns set to unique
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        list_unique_without_index = []
        list_unique_with_index = []
        list_name_columns_with_index = []
        list_type_columns_with_index = []
        list_type_columns_without_index = []
        # separate unique columns that have index tables, from the ones that dont
        for i in range(2, len(data)):
            if data[i]["unique"] == "true":
                if data[i]["index"] == "true":
                    list_unique_with_index.append(i - 2)
                    list_name_columns_with_index.append(data[i]["column_name"])
                    list_type_columns_with_index.append(data[i]["type"])
                else:
                    list_unique_without_index.append(i - 2)
                    list_type_columns_without_index.append(data[i]["type"])
        # check uniqueness of columns without index table
        values_for_unique = self.__get_list_of_values_by_index(table, list_unique_without_index, list_type_columns_without_index)
        for i, index in enumerate(list_unique_without_index):
            if self.__change_type(data_list[index + 1], list_type_columns_without_index[i]) in values_for_unique[index]:
                error_msg = "unique error: the \"" + str(data_list[index + 1]) + "\" data already exists in the database"
                self.__send_msg(error_msg)
                self.send_done = False
                return False
        # check uniqueness of columns with index table
        for i in range(len(list_unique_with_index)):
            index_table_name = "index_" + str(table) + "_" + list_name_columns_with_index[i]
            value = list(self.db[index_table_name].find({"_id": self.__change_type(data_list[list_unique_with_index[i] + 1], list_type_columns_with_index[i])}))
            if len(value) != 0:
                error_msg = "unique error: the \"" + str(data_list[list_unique_with_index[i] + 1]) + "\" data already exists in the database"
                self.__send_msg(error_msg)
                self.send_done = False
                return False
        return True

    def __insert_data_is_correct(self, command_list):
        row_types = self.__get_types_from_table(command_list[1])
        data_list = command_list[2].split("#")
        ok = True
        for type, data in zip(row_types, data_list):
            if type == "int" and not self.__checkInt(data):
                ok = False
            elif type == "float" and not self.__checkFloat(data):
                ok = False
            elif type == "bit" and not self.__checkBit(data):
                ok = False
            elif type == "date" and not self.__checkDate(data):
                ok = False
            elif type == "datetime" and not self.__checkDateTime(data):
                ok = False
            if not ok:
                error_msg = "the inserted data doesen't match the colums' types"
                self.__send_msg(error_msg)
                self.send_done = False
                return False
        if len(row_types) != len(data_list):
            error_msg = "the number of inserted data doesen't match the number of columns in the table"
            self.__send_msg(error_msg)
            self.send_done = False
            return False
        return True

    def __checkDateTime(_, data):
        format = "%Y-%m-%d_%H:%M:%S"
        res = True
        try:
            res = bool(datetime.strptime(data, format))
        except ValueError:
            res = False
        return res

    def __checkDate(_, data):
        format = "%Y-%m-%d"
        res = True
        try:
            res = bool(datetime.strptime(data, format))
        except ValueError:
            res = False
        return res

    def __checkBit(_, data):
        return data == "1" or data == "0"

    def __checkFloat(_, data):
        try:
            float(data)
            return True
        except ValueError:
            return False

    def __checkInt(_, data):
        try:
            int(data)
            return True
        except ValueError:
            return False

    def __get_types_from_table(self, table):
        path = "./" + self.current_db + '/' + table + '.json'
        with open(path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        types = []
        for row in data[1:]:
            types.append(row["type"])
        return types

    def __is_id_in_table(self, table, id):
        collection = self.db[table]
        ids = list(collection.find({"_id": id}))
        return len(ids) != 0

    def __get_list_of_values_by_index(self, table, indexes, types):
        collection = self.db[table]
        list_of_values = []
        values = collection.find({})
        for val in values:
            list_of_values.append(val["Value"])
        values_for_index = {}
        type_for_index = {}
        for i, index in enumerate(indexes):
            values_for_index[index] = []
            type_for_index[index] = types[i]
        for values in list_of_values:
            val = values.split("#")
            for i in indexes:
                values_for_index[i].append(self.__change_type(val[i], type_for_index[i]))
        return values_for_index

    def __change_type(self, value, type):
        match type:
            case 'int':
                return int(value)
            case 'float':
                return float(value)
            case 'bit':
                return int(value)
            case 'date':
                value += "T00:00:00Z"
                myDatetime = dateutil.parser.parse(value)
                return myDatetime
            case 'datetime':
                dt = value.split("_")
                value = dt[0] + "T" + dt[1] + ".000Z"
                myDatetime = dateutil.parser.parse(value)
                return myDatetime
            case _:
                return value

    # adding primary keys, foreign keys, unique keys, indexes

    def __add_primary_key(self, command_list):
        table, column = command_list[2:]
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return
        if not self.__table_exists(table):
            error_msg = table + " doesn't exist"
            self.__send_msg(error_msg)
            self.send_done = False
            return
        if not self.__column_exists(table, column):
            error_msg = table + " doesn't contain the " + column + " column"
            self.__send_msg(error_msg)
            self.send_done = False
            return

        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for pks in data[0]["primary_keys"]:
            if column == pks[0]:
                error_msg = column + " is already a primary key"
                self.__send_msg(error_msg)
                self.send_done = False
                return

        column_index = self.__get_column_index(table, column)
        data[0]["primary_keys"].append([column, column_index])
        data[column_index]["primary_key"] = "true"
        data[column_index]["unique"] = "true"
        data[column_index]["index"] = "true"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def __add_foreign_key(self, command_list):
        table1, column_table1, table2, column_table2 = command_list[2:]

        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return
        if not self.__table_exists(table1):
            error_msg = table1 + " doesn't exist"
            self.__send_msg(error_msg)
            self.send_done = False
            return
        if not self.__table_exists(table2):
            error_msg = table2 + " doesn't exist"
            self.__send_msg(error_msg)
            self.send_done = False
            return
        if not self.__column_exists(table1, column_table1):
            error_msg = table1 + " doesn't contain the " + column_table1 + " column"
            self.__send_msg(error_msg)
            self.send_done = False
            return
        if not self.__column_exists(table2, column_table2):
            error_msg = table2 + " doesn't contain the " + column_table2 + " column"
            self.__send_msg(error_msg)
            self.send_done = False
            return

        path = self.current_db + '/' + table1 + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for fks in data[0]["foreign_keys"]:
            if column_table1 == fks["key"][0]:
                error_msg = column_table1 + " is already a foreign key "
                self.__send_msg(error_msg)
                self.send_done = False
                return

        column_table1_index = self.__get_column_index(table1, column_table1)
        column_table2_index = self.__get_column_index(table2, column_table2)
        column_table2_type = self.__get_column_type(table2, column_table2)
        column_table1_type = data[column_table1_index]["type"]
        if column_table1_type != column_table2_type:
            error_msg = "the type of the foreign key needs to be the same as the one of the column it's referencing"
            self.__send_msg(error_msg)
            self.send_done = False
            return

        data[0]["foreign_keys"].append({"key": [column_table1, column_table1_index, column_table1_type], "table": table2, "column": [column_table2, column_table2_index, column_table2_type]})
        data[column_table1_index]["foreign_key"] = "true"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        path = self.current_db + '/' + table2 + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data[0]["child_tables"].append({"key": [column_table2, column_table2_index, column_table2_type], "table": table1, "column": [column_table1, column_table1_index, column_table1_type]})
        data[column_table2_index]["parent_table"] = "true"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def __add_unique_key(self, command_list):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if not self.__table_exists(command_list[2]):
            self.__send_msg("Table doesn't exist")
            self.send_done = False
            return

        if not self.__column_exists(command_list[2], command_list[3]):
            self.__send_msg("Column doesn't exist")
            self.send_done = False
            return

        path = self.current_db + '/' + command_list[2] + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        column = self.__get_column_index(command_list[2], command_list[3])
        data[column]['unique'] = 'true'

        with open(path, "w", encoding="utf-8") as g:
            json.dump(data, g, indent=4)

    def __add_index(self, command_list):
        table, column = command_list[2:]
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return

        if not self.__table_exists(table):
            self.__send_msg("Table doesn't exist")
            self.send_done = False
            return

        if not self.__column_exists(table, column):
            error_msg = table + " doesn't contain the " + column + " column"
            self.__send_msg(error_msg)
            self.send_done = False
            return

        index_table_name = "index_" + str(table) + "_" + str(column)
        if self.__table_exists(index_table_name):
            self.__send_msg("Index table already exists")
            self.send_done = False
            return

        # setting the corresponding value to true in the json file
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        column_index = self.__get_column_index(table, column)
        if data[column_index]["index"] == "true":
            self.__send_msg("the " + str(column) + " of the " + str(table) + " already has an index file")
            self.send_done = False
            return
        else:
            data[column_index]["index"] = "true"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        # creating an index file in mongodb for the column
        db = self.client[self.current_db]
        db.create_collection(index_table_name)

    def __get_column_index(self, table, column):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for i in range(1, len(data)):
            if data[i]["column_name"] == column:
                return i

    def __get_column_type(self, table, column):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for i in range(1, len(data)):
            if data[i]["column_name"] == column:
                return data[i]["type"]

    def __get_id_column_name(self, table):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data[1]['column_name']
        

    # check if database, table and column exists

    def __database_exists(self, db_name):
        database_names = []
        for db in self.client.list_databases():
            database_names.append(db.get("name"))
        return db_name in database_names and os.path.exists(db_name)

    def __table_exists(self, table_name):
        db = self.client[self.current_db]
        table_names = db.list_collection_names()
        path = self.current_db + '/' + table_name + '.json'

        return table_name in table_names and os.path.exists(path)

    def __column_exists(self, table, column):
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for col in data[1:]:
            if col["column_name"] == column:
                return True
        return False

    # deleting the pinning collection after a table is created in the database

    def __delete_pinning_collection(self):
        db = self.client[self.current_db]
        db.drop_collection("database created")

    # returns list with table names in order

    def __get_column_names(self, table):
        if self.current_db is None:
            self.__send_msg("Choose a database")
            self.send_done = False
            return
        
        path = self.current_db + '/' + table + '.json'
        data = []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        column_names = []
        for colums in data[1:]:
            column_names.append(colums["column_name"])
        return column_names



def parse_args() -> str | None:
    if len(sys.argv) == 1:
        return None
    elif len(sys.argv) == 3 and sys.argv[1] in ("-p", "--password"):
        return sys.argv[2]
    else:
        print("Error: bad args")
        exit(1)


def main(passwd: str = None):
    server = Server(passwd)
    server.run()


if __name__ == "__main__":
    passwd = parse_args()
    main(passwd)


# SELECT p.product_name, c.category_name, p.price FROM products p INNER JOIN categories c ON c.category_id = p.category_id WHERE p.price > 100