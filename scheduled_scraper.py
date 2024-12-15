import requests  #Библиотека для выполнения HTTP-запросов
from bs4 import BeautifulSoup  #Библиотека для парсинга HTML
import pandas as pd  #Библиотека для работы с табличными данными (DataFrame)
import schedule  #Библиотека для планирования задач
import time  #Библиотека для работы со временем
import csv  #Библиотека для работы с CSV-файлами
from datetime import datetime  #Библиотека для работы с датой и временем

base_url = 'http://books.toscrape.com/catalogue/' #URL каталога с книагами

#Функция собирает данные о книгах с 50 страниц сайта и сохраняет в CSV-файл.
def scrape_books():
    all_books = [] #Список для хранения данных о всех книгах сайта
    print(f"Начало сбора данных: {datetime.now()}")  # Выводим время начала сбора данных
    for page in range(1, 51):  #Цикл для 50 страниц сайта
        url = base_url + f"page-{page}.html" #Формируем URL текущей страницы каталога
        print(f"Обработка страницы: {url}") #Вывод URL обрабатываемой страницы
        response = requests.get(url) #Отправляем GET-запрос
    
        if response.status_code != 200: #Если статус-код не 200
          print(f"Ошибка при загрузке страницы {url}. Код ответа: {response.status_code}") #Вывод сообщения об ошибке
          continue #Переход к следующей странице
      
        soup = BeautifulSoup(response.text, 'html.parser') #Создаем объект BeautifulSoup
    
        for article in soup.find_all('article', class_='product_pod'): #Добавляем цикл по всем книгам на текущей странице
            title = article.h3.a['title'] #Получаем название книги
            book_url = base_url + article.h3.a['href'] #Формируем URL страницы книги
            book_response = requests.get(book_url) #Отправляем GET-запрос
        
            if book_response.status_code != 200: #Если статус-код не 200
              print(f"Ошибка при загрузке страницы книги {book_url}. Код ответа: {book_response.status_code}")  #Вывод сообщения об ошибке
              continue #Переход к следующей книге
          
            book_soup = BeautifulSoup(book_response.text, 'html.parser') #Создаем объект BeautifulSoup для страницы книги
            price = book_soup.find('p', class_='price_color').text #Получаем цену книги
            availability = book_soup.find('p', class_='instock availability').text.strip() #Получаем информацию о наличии книги (остатках)
            rating = book_soup.find('p', class_='star-rating')['class'][1] #Получаем рейтинг книги
            description = book_soup.find('meta', attrs={'name': 'description'})['content'].strip() #Получаем описание книги
            product_info = {} #Создаем пустой словарь для дополнительной информации о книге
            for row in book_soup.find('table', class_='table table-striped').find_all('tr'): #Добавляем цикл по всем строкам таблицы с дополнительной информацией
              header = row.find('th').text.strip() #Получаем заголовок строки
              value = row.find('td').text.strip() #Получаем значение строки
              product_info[header] = value #Добавляем в словарь
        
            all_books.append({'title': title,'availability': availability,'rating': rating,'description': description,**product_info}) #Добавляем информацию о книге в список

    #Создание DataFrame
    df = pd.DataFrame(all_books)

    #Предобработка данных
    df = df.drop('UPC', axis=1, errors='ignore')  #Удаление столбца UPC, если столбец есть
    df = df.drop('Product Type', axis=1, errors='ignore')  #Удаление столбца Product Type, если столбец есть
    df = df.drop('Price (excl. tax)', axis=1, errors='ignore')  #Удаление столбца Price (excl. tax), если столбец есть
    df = df.drop('Tax', axis=1, errors='ignore')  #Удаление столбца Tax, если столбец есть
    df = df.drop('Number of reviews', axis=1, errors='ignore')  #Удаление столбца Number of reviews, если столбец есть
    df = df.drop('Availability', axis=1, errors='ignore')  #Удаление столбца Availability, если столбец есть


    df = df.dropna() #Удаление строк с пропусками
    df=df.drop_duplicates() #Удаление дубликатов

    all_books=df.shape[0] #Общее количество книг в собранных данных
    print(f'Общее количество книг:{all_books}') #Вывод общего количества книг в собранных данных
    statistics=df.describe() #Основные статистики по числовым данным
    print(f"Статистика:\n{statistics}") #Вывод основных статистик по числовым данным 


    df.to_csv('books_data.csv', index=False) #Сохранение в CSV
    print("Данные успешно сохранены в books_data.csv") #Вывод сообщения о сохранении данных в файл


# Настройка расписания
schedule.every().day.at("19:00").do(scrape_books)  #Запланирован автоматический запуск функции scrape_books каждый день в 19:00

#Бесконечный цикл для ожидания и выполнения задач по расписанию
while True:
    schedule.run_pending()  #Проверяем, есть ли запланированные задачи для выполнения
    time.sleep(1)  #Приостанавливаем выполнение на 1 секунду
