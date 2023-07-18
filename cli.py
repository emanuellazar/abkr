import socket

import parse
from type_def import Types
import tabulate as tb

PORT = 43569
HOST = "localhost"
BUFF_SIZE = 1024


class CLI:
    '''
        CLI class
    '''

    current_db = None

    def __init__(self):
        self.command_list = []

    def run(self):
        '''
            Starts the CLI
        '''

        while True:
            command = input(f"{self.current_db}> ")
            self.command_list = parse.tokenize(command)

            if len(self.command_list) == 0:
                continue

            # exit
            if len(self.command_list
                   ) == 1 and self.command_list[0] == Types.EXIT:
                break

            try:
                self.__check_then_run()

                # use
                if len(self.command_list
                       ) == 2 and self.command_list[0] == Types.USE:
                    self.current_db = self.command_list[1]
                # drop
                if (len(self.command_list) == 3
                        and self.command_list[0] == Types.DROP
                    and self.command_list[1] == Types.DATABASE
                        and self.current_db == self.command_list[2]):
                    self.current_db = None

            except Exception as e:
                print(f"Error. {e}")

    def __check_then_run(self):
        '''
            Check for the correctness of the command list
            if correct run
        '''
        if parse.parse(self.command_list):

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP socket
            s.connect((HOST, PORT))

            string_command_list = self.__list_to_string()

            s.send(string_command_list.encode())

            response = s.recv(BUFF_SIZE).decode()
            s.close()

            if response == "done":
                return

            if response.split(' ')[0] == 'TABLE':
                print(response)
                self.__print_table(response)
                return

            raise Exception(response)
        else:
            raise Exception("Wrong input.")

    def __list_to_string(self) -> str:

        def merge(str_list: list) -> str:
            string = ""
            for elem in str_list:
                string += elem + "#"
            string = string[:-1]

            return string

        string_command_list = ""

        # if needs param merge
        if self.command_list[0] == Types.INSERT:
            merged = merge(self.command_list[4:])
            string_command_list = str(
                self.command_list[0].value
            ) + " " + self.command_list[2] + " " + merged
            return string_command_list

        for idx, command in enumerate(self.command_list):
            if isinstance(command, Types):
                string_command_list += str(command.value) + " "
                if command == Types.SELECT and self.command_list[idx + 1]\
                        != Types.ALL:
                    string_command_list += str(Types.COLUMNS.value) + " "
            else:
                string_command_list += command + " "
        string_command_list = string_command_list[:-1]

        return string_command_list

    def __print_table(self, table: str) -> None:
        table_split = table.split(' ')
        nr = int(table_split[1])

        with open('select.txt', 'r') as f:
            data = f.read().replace('\n', '')
        data = data.split(" ")
        d = [data[i: i + nr] for i in range(0, len(data), nr)]
        final_table = []
        for elem in d:
            if elem not in final_table:
                final_table.append(elem)
        print (tb.tabulate(final_table[1:], headers=final_table[0]))
