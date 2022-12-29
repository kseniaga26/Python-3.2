import concurrent.futures as pool
import csv
import math
import os
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from jinja2 import Template
import pdfkit

currency_to_rub = {
    "AZN": 35.68,
    "BYR": 23.91,
    "EUR": 59.90,
    "GEL": 21.74,
    "KGS": 0.76,
    "KZT": 0.13,
    "RUR": 1,
    "UAH": 1.64,
    "USD": 60.66,
    "UZS": 0.0055}

class InputCorrect:
    def __init__(self, file: str, prof: str):
        self.file_name = file
        self.prof = prof
        self.check_file()

    def check_file(self) -> None:
        with open(self.file_name, "r", encoding='utf-8-sig', newline='') as csv_file:
            file_iter = iter(csv.reader(csv_file))
            if next(file_iter, "none") == "none":
                print("Пустой файл")
                exit()


class Salary:
    def __init__(self, dictionary):
        self.salary_from = math.floor(float(dictionary["salary_from"]))
        self.salary_to = math.floor(float(dictionary["salary_to"]))
        self.salary_currency = dictionary["salary_currency"]
        middle_salary = (self.salary_to + self.salary_from) / 2
        self.salary_in_rur = currency_to_rub[self.salary_currency] * middle_salary


class Vacancy:
    def __init__(self, dictionary: dict):
        self.dictionary = dictionary
        self.salary = Salary(dictionary)
        self.dictionary["year"] = int(dictionary["published_at"][:4])
        self.is_needed = dictionary["is_needed"]


class DataSet:
    def __init__(self, csv_dir: str, prof: str, file_name: str):
        self.csv_dir = csv_dir
        self.prof = prof
        self.start_line = []
        self.year_to_count = {}
        self.year_to_salary = {}
        self.year_to_count_needed = {}
        self.year_to_salary_needed = {}
        self.area_to_salary = {}
        self.area_to_piece = {}
        area_to_sum, area_to_count = self.csv_divide(file_name)
        self.count_area_data(area_to_sum, area_to_count)
        self.sort_year_dicts()

    def csv_reader(self, read_queue: list) -> None:
        for data in read_queue:
            self.year_to_count[data[0]] = data[1]
            self.year_to_salary[data[0]] = data[2]
            self.year_to_count_needed[data[0]] = data[3]
            self.year_to_salary_needed[data[0]] = data[4]

    def save_file(self, current_year: str, lines: list) -> str:
        file_name = f"file_{current_year}.csv"
        with open(f"{self.csv_dir}/{file_name}", "w", encoding='utf-8-sig', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(lines)
        return file_name

    def try_to_add(dic: dict, key, val) -> dict:
        try:
            dic[key] += val
        except:
            dic[key] = val
        return dic

    def read_one_csv_file(self, file_name: str) -> list:
        print("start: "+file_name)
        with open(f"{self.csv_dir}/{file_name}", "r", encoding='utf-8-sig', newline='') as csv_file:
            file = csv.reader(csv_file)
            filtered_vacs = []
            year = int(file_name.replace("file_", "").replace(".csv", ""))
            for line in file:
                new_dict_line = dict(zip(self.start_line, line))
                new_dict_line["is_needed"] = (new_dict_line["name"]).find(self.prof) > -1
                vac = Vacancy(new_dict_line)
                filtered_vacs.append(vac)
            csv_file.close()
            all_count = len(filtered_vacs)
            all_sum = sum([vac.salary.salary_in_rur for vac in filtered_vacs])
            all_middle = math.floor(all_sum / all_count)
            needed_vacs = list(filter(lambda vacancy: vacancy.is_needed, filtered_vacs))
            needed_count = len(needed_vacs)
            needed_sum = sum([vac.salary.salary_in_rur for vac in needed_vacs])
            needed_middle = math.floor(needed_sum / needed_count)
        print("stop: " + file_name)
        return [year, all_count, all_middle, needed_count, needed_middle]

    def csv_divide(self, file_name: str):
        area_to_sum = {}
        area_to_count = {}
        with open(file_name, "r", encoding='utf-8-sig', newline='') as csv_file:
            all_files = []
            file = csv.reader(csv_file)
            self.start_line = next(file)
            year_index = self.start_line.index("published_at")
            next_line = next(file)
            current_year = int(next_line[year_index][:4])
            data_years = [next_line]
            for line in file:
                if not ("" in line) and len(line) == len(self.start_line):
                    new_dict_line = dict(zip(self.start_line, line))
                    new_dict_line["is_needed"] = None
                    vac = Vacancy(new_dict_line)
                    area_to_sum = DataSet.try_to_add(area_to_sum, vac.dictionary["area_name"], vac.salary.salary_in_rur)
                    area_to_count = DataSet.try_to_add(area_to_count, vac.dictionary["area_name"], 1)
                    if vac.dictionary["year"] != current_year:
                        new_csv = self.save_file(current_year, data_years)
                        all_files.append(new_csv)
                        data_years = []
                        print("save " + str(current_year))
                        current_year = vac.dictionary["year"]
                    data_years.append(line)
            new_csv = self.save_file(str(current_year), data_years)
            all_files.append(new_csv)
            with pool.ThreadPoolExecutor(max_workers=16) as executer:
                res = executer.map(self.read_one_csv_file, all_files)
            read_queue = list(res)
            csv_file.close()
            self.csv_reader(read_queue)
            return area_to_sum, area_to_count

    def get_sorted_dict(key_to_salary: dict) -> dict:
        return dict(list(sorted(key_to_salary.items(), key=lambda item: item[1], reverse=True))[:10])

    def sort_dict_for_keys(dictionary: dict) -> dict:
        return dict(sorted(dictionary.items(), key=lambda item: item[0]))

    def sort_year_dicts(self):
        self.year_to_count = DataSet.sort_dict_for_keys(self.year_to_count)
        self.year_to_salary = DataSet.sort_dict_for_keys(self.year_to_salary)
        self.year_to_count_needed = DataSet.sort_dict_for_keys(self.year_to_count_needed)
        self.year_to_salary_needed = DataSet.sort_dict_for_keys(self.year_to_salary_needed)
        self.area_to_piece = DataSet.get_sorted_dict(self.area_to_piece)
        self.area_to_salary = DataSet.get_sorted_dict(self.area_to_salary)

    def get_middle_salary(key_to_count: dict, key_to_sum: dict) -> dict:
        key_to_salary = {}
        for key, val in key_to_count.items():
            if val == 0:
                key_to_salary[key] = 0
            else:
                key_to_salary[key] = math.floor(key_to_sum[key] / val)
        return key_to_salary

    def get_area_to_salary_and_piece(area_to_sum: dict, area_to_count: dict) -> tuple:
        vacs_count = sum(area_to_count.values())
        area_to_count = dict(filter(lambda item: item[1] / vacs_count > 0.01, area_to_count.items()))
        area_to_middle_salary = DataSet.get_middle_salary(area_to_count, area_to_sum)
        area_to_piece = {key: round(val / vacs_count, 4) for key, val in area_to_count.items()}
        return area_to_middle_salary, area_to_piece

    def count_area_data(self, area_to_sum: dict, area_to_count: dict) -> None:
        self.area_to_salary, self.area_to_piece = \
            DataSet.get_area_to_salary_and_piece(area_to_sum, area_to_count)

class Report:
    def __init__(self, data: DataSet):
        self.data = data
        self.years_sheet_headers = ["Год", "Средняя зарплата", "Средняя зарплата - Программист",
                                    "Количество вакансий", "Количество вакансий - Программист"]
        years_sheet_columns = [list(data.year_to_salary.keys()), list(data.year_to_salary.values()),
                               list(data.year_to_salary_needed.values()), list(data.year_to_count.values()),
                               list(data.year_to_count_needed.values())]
        self.years_sheet_rows = self.get_table_rows(years_sheet_columns)
        self.city_sheet_headers = ["Город", "Уровень зарплат", " ", "Город", "Доля вакансий"]
        city_sheet_columns = [list(data.area_to_salary.keys()), list(data.area_to_salary.values()),
                              ["" for _ in data.area_to_salary.keys()], list(data.area_to_piece.keys()),
                              list(map(self.get_percents, data.area_to_piece.values()))]
        self.city_sheet_rows = self.get_table_rows(city_sheet_columns)

    def get_percents(value):
        return f"{round(value * 100, 2)}%"

    def get_table_rows(columns: list) -> list:
        rows_list = [["" for _ in range(len(columns))] for _ in range(len(columns[0]))]
        for col in range(len(columns)):
            for cell in range(len(columns[col])):
                rows_list[cell][col] = columns[col][cell]
        return rows_list

    def create_regular_schedule(self, ax: Axes, keys1, keys2, values1, values2, label1, label2, title) -> None:
        x1 = [key - 0.2 for key in keys1]
        x2 = [key + 0.2 for key in keys2]
        ax.bar(x1, values1, width=0.4, label=label1)
        ax.bar(x2, values2, width=0.4, label=label2)
        ax.legend()
        ax.set_title(title, fontsize=16)
        ax.grid(axis="y")
        ax.tick_params(axis='x', labelrotation=90)

    def create_horizontal_schedule(self, ax: Axes) -> None:
        ax.set_title("Уровень зарплат по городам", fontsize=16)
        ax.grid(axis="x")
        keys = [key.replace(" ", "\n").replace("-", "-\n") for key in list(self.data.area_to_salary.keys())]
        ax.barh(keys, self.data.area_to_salary.values())
        ax.tick_params(axis='y', labelsize=6)
        ax.set_yticks(keys)
        ax.set_yticklabels(labels=keys, verticalalignment="center", horizontalalignment="right")
        ax.invert_yaxis()

    def create_pie_schedule(self, ax: Axes, plt) -> None:
        ax.set_title("Доля вакансий по городам", fontsize=16)
        plt.rcParams['font.size'] = 8
        dic = self.data.area_to_piece
        dic["Другие"] = 1 - sum([val for val in dic.values()])
        keys = list(dic.keys())
        ax.pie(x=list(dic.values()), labels=keys)
        ax.axis('equal')
        ax.tick_params(axis="both", labelsize=6)
        plt.rcParams['font.size'] = 16

    def generate_schedule(self, file_name: str) -> None:
        fig, axis = plt.subplots(2, 2)
        plt.rcParams['font.size'] = 8
        self.create_regular_schedule(axis[0, 0], self.data.year_to_salary.keys(),
                                     self.data.year_to_salary_needed.keys(),
                                     self.data.year_to_salary.values(), self.data.year_to_salary_needed.values(),
                                     "Средняя з/п", "з/п программист", "Уровень зарплат по годам")
        self.create_regular_schedule(axis[0, 1], self.data.year_to_count.keys(), self.data.year_to_count_needed.keys(),
                                     self.data.year_to_count.values(), self.data.year_to_count_needed.values(),
                                     "Количество вакансий", "Количество вакансий программист",
                                     "Количество вакансий по годам")
        self.create_horizontal_schedule(axis[1, 0])
        self.create_pie_schedule(axis[1, 1], plt)
        fig.set_size_inches(16, 9)
        fig.tight_layout(h_pad=2)
        fig.savefig(file_name)

    def generate_pdf(self, file_name: str):
        image_name = "graph.png"
        self.generate_schedule(image_name)
        html = open("pdf_template.html").read()
        template = Template(html)
        keys_to_values = {
            "prof_name": "Аналитика по зарплатам и городам для профессии " + self.data.prof,
            "image_name": "C:/Users/Shira/PycharmProjects/pythonProject_4/pythonProject_6_3/" + image_name,
            "year_head": "Статистика по годам",
            "city_head": "Статистика по городам",
            "years_headers": self.years_sheet_headers,
            "years_rows": self.years_sheet_rows,
            "cities_headers": self.city_sheet_headers,
            "count_columns": len(self.city_sheet_headers),
            "cities_rows": self.city_sheet_rows
        }
        pdf_template = template.render(keys_to_values)
        config = pdfkit.configuration(wkhtmltopdf=r"D:\For PDF python\wkhtmltopdf\bin\wkhtmltopdf.exe")
        pdfkit.from_string(pdf_template, file_name, configuration=config, options={"enable-local-file-access": True})

def create_pdf(csv_dir: str, file_name: str):
    file_csv_name = input("Введите название файла: ")
    prof = "Аналитик"
    if os.path.exists(csv_dir):
        import shutil
        shutil.rmtree(csv_dir)
    os.mkdir(csv_dir)
    data_set = DataSet(csv_dir, prof, file_csv_name)
    report = Report(data_set)
    report.generate_pdf(file_name)


if __name__ == '__main__':
    create_pdf("csv", "report323.pdf")