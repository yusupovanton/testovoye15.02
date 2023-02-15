package main

import (
	"context"
	"database/sql" // Допустим что query используется в plain sql, а не в ORM
	"runtime"
	"sync"
)

type Result struct{
    Rows sql.Rows // В результате одно из полей скорее всего будет строками из БД
}

type ErrVal struct { // Защитим значение ошибки через мютекс
    err error
    mu *sync.Mutex
}

func errWrite(errVal ErrVal, err error) { // Функция для записи в защищенную переменную error
    defer errVal.mu.Unlock()

    errVal.mu.Lock()
    errVal.err = err
}

type SearchFunc func(ctx context.Context, query string) (Result, error) // Тип функции

func MultiSearch(ctx context.Context, query string, sfs []SearchFunc) (Result, error) {

    // ТЗ
    // Нужно реализовать функцию, которая выполняет поиск query во всех переданных SearchFunc
    // Когда получаем первый успешный результат - отдаем его сразу. Если все SearchFunc отработали
    // с ошибкой - отдаем последнюю полученную ошибку

    // Управление ядрами
    maxProcs := 4
    runtime.GOMAXPROCS(maxProcs)

    // Управление памятью (реализовано с помощью канала, контроллирующего количество горутин)
    maxRoutines := 10 // Например, только 10 горутин создаются одновременно в памяти
    goroutines := make(chan struct{}, maxRoutines)

    var wg sync.WaitGroup
    var errVal ErrVal

    wg.Add(len(sfs)) // Добавляем столько горутин, сколько у нас функций

    ctx, cancel := context.WithCancel(ctx) // Добавление функционала cancel
    defer cancel() // В случае, если все горутины отработали с ошибками, надо закрыть контекст

    chanRes := make(chan Result, 1) // Канал для результата (закрывается после 1 значения)
	
    for _, function := range sfs { // Для каждой функции добавляем горутину в scheduler

        go func(ctx context.Context, query string, function SearchFunc, errVal ErrVal, goroutines chan struct{}) {
            
            defer wg.Done() 
            goroutines <- struct{}{} // резервируем место под горутину
            
            select {
            case <-ctx.Done(): // Если наша горутина получила сигнал извне о том, что надо остановиться...
                break // ... Завершим горутину без выполнения квери

            default: // В остальных случаях вызываем квери
                result, err := function(ctx, query) // перформим квери и записываем результат в локальные переменные

                if err == nil { // нет ошибки значит квери вернула значение - сразу записываем значение
                    chanRes <- result  // записываем значение результата в канал, он закрывается
                    cancel() // передаем сигнал остановки всем остальным горутинам
                }

                // !! Тут я беру за данное, то что не найдя строк внешний ресурс даст ошибку! Если этого
                // не происходит - надо проверять на наличие строк в Result.Rows. Процесс будет зависеть от 
                // конкретного ресурса, но в общем процесс такой

                errWrite(errVal, err) // пишем значение ошибки в переменную с мьютексом
                
            }

            <- goroutines // отпускаем горутину чтобы иметь возможность создать другую

        }(ctx, query, function, errVal, goroutines)
    }

    wg.Wait() // Ждем выполнения горутин

    // В случае если горутиины отработали с ошибкой - канал результата пуст, а канал ошибок заполнен ошибками
    // В случае, если результат был положительный - в канале результата одно значение

    if len(chanRes) == 0 { // Вернется последняя ошибка и пустая структура результата
        return Result{}, errVal.err

    } else { // Вернется результат и ошибка == nil
        result := <-chanRes
        return result, nil
    }
    
}
