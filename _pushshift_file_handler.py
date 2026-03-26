import csv
import os
from dataclasses import dataclass


@dataclass
class file_handler:
    """
    Class for handling data files.
    ----------
    methods
    ----------
    combine: combine multiple csv files into one.
    """

    def __init__(self):
        """
        create handler obeject.
        """

    def combine(self, headers=None, files=None, path_out=None):
        """
        function for combining multiple files.
        ----------
        paramaters
        ----------
        headers: headers to write to file, pass as list
        files: list of paths to files to combine
        path_out: path to output combined csv to.
        """

        self.headers = headers
        self.files = files
        self.path_out = path_out
        if self.path_out == None:
            self.path_out = os.getcwd()
        self.log = [
            "files = {}".format(self.files),
            "path_out = {}".format(self.path_out),
        ]
        self.errors = 0
        self.written = 0
        with open(self.path_out, "w", newline="", encoding="utf-8") as out_file:
            writer = csv.writer(out_file, delimiter=",")
            if self.headers != None:
                writer.writerow(self.headers)
            if self.files == None:
                print(
                    'file list is empty! please specify files=["file_1", ... , "file_n"]'
                )
            for file in self.files:
                with open(file, "r", newline="", encoding="utf-8") as in_file:
                    reader = csv.reader(in_file, delimiter=",")
                    next(reader, None)  # skip the headers
                    for row in reader:
                        try:
                            writer.writerow(row)
                            self.written += 1
                        except csv.Error:
                            self.log.append(row)
                            self.errors += 1
        self.log.append("rows written = {}".format(self.written))
        self.log.append("erraneous rows = {}".format(self.errors))
        print(
            "file combined, {} rows written, {} errors found. check file_handler.log for more details.".format(
                self.written, self.errors
            )
        )


__all__ = ["file_handler"]
