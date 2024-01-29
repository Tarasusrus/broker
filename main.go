package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message - структура, представляющая сообщение,
// которое должно быть передано через очередь.
// Сообщение содержит только одно поле - значение сообщения.
type Message struct {
	Value string // Значение сообщения
}

// Queue - структура, представляющая очередь сообщений. Каждая очередь содержит
// список сообщений и условную переменную, которая используется для синхронизации
// процесса добавления и извлечения сообщений из очереди.
type Queue struct {
	sync.RWMutex            // Блокировка чтения-записи для синхронизации доступа к очереди
	Messages     []Message  // Список сообщений в очереди
	cond         *sync.Cond // Условная переменная для синхронизации процесса добавления и извлечения сообщений
}

// queues - это глобальный словарь всех очередей. Ключом является имя очереди, а значением - ссылка на объект Queue
var (
	queues     = make(map[string]*Queue) // Словарь всех очередей
	queueMutex sync.Mutex                // Блокировка для синхронизации изменений словаря очередей
)

func enqueue(chanelName string, message Message) {
	// проверяем что канал существует
	queueMutex.Lock()
	chanel, exist := queues[chanelName]
	// если не существует добавляем канал
	if !exist {
		chanel = &Queue{
			Messages: []Message{},
			cond:     sync.NewCond(&sync.Mutex{}),
		}
		queues[chanelName] = chanel

	}
	queueMutex.Unlock()
	// добавляем сообщение в канал
	chanel.Lock()
	defer chanel.Unlock()
	chanel.Messages = append(chanel.Messages, message)
	// когда сообщение добавляется в очередь будем пробуждать горутину
	chanel.cond.Signal()

}

func getQueueMes(chanelName string) (message Message, err error) {
	// проверяем что канал существует
	chanel, exist := queues[chanelName]
	if !exist {
		return Message{}, errors.New("Queue does not exist")
	}

	chanel.RLock()
	// проверяем что сообщения в канале есть
	if len(chanel.Messages) == 0 {
		chanel.RUnlock() // Если условие истина, то нужно сначала разблокировать RLock
		return Message{}, errors.New("Queue is empty")
	}
	// fifo это очередь. вернули нулевой индекс затем удалили его -> очередь сдвинулась
	message = chanel.Messages[0]
	chanel.RUnlock() // Разблокировка чтения после того, как операция чтения завершена
	chanel.Lock()
	chanel.Messages = chanel.Messages[1:]
	chanel.Unlock()
	return message, nil

}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		var queueName = strings.TrimPrefix(r.URL.Path, "/") // Узнаем имя очереди из URL
		var value = r.URL.Query().Get("v")                  // Получим значение сообщения из GET параметра

		// Если значение не было передано, вернём ошибку
		if value == "" {
			http.Error(w, "Value is empty", http.StatusNotFound)
			return
		}
		message := Message{Value: value} // Создаём новое сообщение
		enqueue(queueName, message)      // Добавляем сообщение в очередь

	case http.MethodGet:
		var queueName = strings.TrimPrefix(r.URL.Path, "/") // Узнаем имя очереди из URL
		timeout := r.URL.Query().Get("timeout")             // Проверяем, есть ли параметр таймаута в запросе
		timeoutInt, err := strconv.Atoi(timeout)
		if err != nil {
			// Если таймаут не был задан, используем функцию getQueueMes напрямую
			message, err := getQueueMes(queueName)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound) // Если возникла ошибка, возвращаем 404 статус
				return
			}
			fmt.Fprintf(w, "%s", message.Value) // Возвращаем значение сообщения
		} else {
			// Если таймаут был установлен, используем функцию getQueueMessageWithTimeout
			message, err := getQueueMessageWithTimeout(queueName, time.Duration(timeoutInt)*time.Second)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound) // Если возникла ошибка, возвращаем 404 статус
				return
			}
			fmt.Fprintf(w, "%s", message.Value) // Возвращаем значение сообщения
		}
	}
}

func getQueueMessageWithTimeout(chName string, timeout time.Duration) (msg Message, err error) {
	ch, exist := queues[chName] // Получаем очередь по имени
	if !exist {                 // Если очереди не существует, возвращаем ошибку
		return Message{}, errors.New("Queue does not exist")
	}
	ch.RLock()
	if len(ch.Messages) == 0 { // Если в очереди нет сообщений, ожидаем появления сообщения или истечения таймаута
		ch.RUnlock()
		timer := time.NewTimer(timeout) // Устанавливаем таймер на заданный таймаут
		for {                           // Начинаем бесконечный цикл
			// Ждем появления нового сообщения или истечения времени ожидания
			ch.cond.L.Lock()
			ch.cond.Wait()
			ch.cond.L.Unlock()

			ch.RLock()
			if len(ch.Messages) == 0 { // Если сообщений по-прежнему нет
				ch.RUnlock()
				select {
				case <-timer.C: // Если время ожидания истекло, возвращаем ошибку
					return Message{}, errors.New("Timeout waiting for message")
				default:
					continue // Если время еще не истекло, продолжаем ожидание
				}
			} else { // Если есть сообщения
				msg = ch.Messages[0] // Берем первое сообщение
				ch.RUnlock()

				ch.Lock()
				ch.Messages = ch.Messages[1:] // Удаляем его из очереди
				ch.Unlock()
				timer.Stop() // Останавливаем таймер
				return msg, nil
			}
		}
	}
	msg = ch.Messages[0] // Если сообщение уже было в очереди, берем его
	ch.RUnlock()

	ch.Lock()
	ch.Messages = ch.Messages[1:] // Удаляем сообщение из очереди
	ch.Unlock()
	return msg, nil
}

func main() {
	port := flag.String("port", "8080", "Default server port")
	flag.Parse()
	http.HandleFunc("/", handleRequest)

	err := http.ListenAndServe(":"+*port, nil)
	if err != nil {
		panic(err)
	}
}
