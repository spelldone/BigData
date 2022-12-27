# Лабораторная работа №4 - ZooKeeper
## Задание
+ запустить ZooKeeper,
+ изучить директорию с установкой ZooKeeper,
+ запустить интерактивную сессию ZooKeeper CLI и освоить её команды,
+ научиться проводить мониторинг ZooKeeper,
+ разработать приложение с барьерной синхронизацией, основанной на ZooKeeper,
+ запустить и проверить работу приложения.

## Установка и запуск
Для установки использовался приведенный в задании алгоритм:
Был скачан архив, затем распакован в папке TEMP, изменено название конфиг файла и произведен запуск командой zkServer.cmd <br/>
<img src="/LR4/images/install.png"> <br/>

## Изучение директории
Перейдем в директорию установки ZooKeeper и изучим содержимое директории. <br/>
В директории находятся следующие папки:
+ bin с исполняемыми файлами для запуска, остановки и взаимодействия с ZooKeeper,
+ conf с конфигурационными файлами,
+ contrib с инструментами для интеграции ZooKeeper в другие системы: rest, fuse, perl и python библиотеки,
+ dist-maven артефакты Maven,
+ docs в которой хранится документация,
+ recipes различные рецепты, помогающие решать задачи с использованием ZooKeeper (выбор лидера, блокировки, очереди),
+ src с исходным кодом и тестовыми скриптами.

## Запуск интерактивной сессии ZooKeeper CLI, освоение команд
Для запуска интерактивной сессии ZooKeeper CLI используем скрипт zkCli.
Следующая команда устанавливает подключение к ZooKeeper CLI сессии: <br/>
<img src="/LR4/images/zkCli.png"> <br/>
Подключение установлено. Для вывода всех возможных команд наберем ```help```:
<img src="/LR4/images/help.png"> <br/>
Находясь в консоли CLI введем команду ```ls /```.
В результе получим список узлов в корне иерархической структуры данных ZooKeeper. В данном случае выводится один узел. Аналогично можно изучать некорневые узлы. 
Теперь в корне создим свой узел ```/mynode``` с данными ```"first_version"``` следующей командой ```create /mynode 'first_version'```.
Проверяем, что в корне появился новый узел: <br/>
Изменим данные узла на ```"second_version"```: <br/>
<img src="/LR4/images/work.png"> <br/>
Создадим два нумерованных (sequential) узла в качестве дочерних ```mynode```: <br/>
<br/>
<img src="/LR4/images/create.png"> <br/>
<br/>
Передав дополнительно флаг ```-s```, мы указали, что создаваемый узел нумерованный. Этот способ позволяет создавать узлы с уникальными именами, по которым можно узнать порядок поступления запросов на сервер. <br/>
Пример. Принадлежность клиентов к группе. <br/>
Несмотря на то, что ZooKeeper используется, как правило, из программного кода, мы можем эмулировать простой сценарий мониторинга принадлежности клиентов к группе в CLI. <br/>
В данном примере в корне zookeeper файловой системы будет создан узел под именем mygroup. Затем несколько сессий CLI будут эмулировать клиентов, добавляющих себя в эту группу. Клиент будет добавлять эфимерный узел в mygroup иерархию. При закрытии сессии узел автоматически будет удаляться.
Этот сценарий может применяться для реализации сервиса разрешения имён (DNS) узлов кластера. Каждый узел регистрирует себя под своим именем и сохраняет свой url или ip адрес. Узлы, которые временно недоступны или аварийно завершили работу, в списке отсутствуют. Таким образом директория хранит актуальный список работающих узлов с их адресами. <br/>
Внутри CLI сессии, создадим узел ```mygroup```. 
Откроем две новых CLI консоли и в каждой создайте по дочернему узлу в ```mygroup``` и проверим, что ```grue``` и ```bleen``` являются членами группы ```mygroup```: <br/>
Представим теперь, что одному из клиентов нужна информация о другом клиенте (к качестве клиентов могут выступать узлы кластера). Этот сценарий эмулируется получением информации командой get. Выберем консоль ```grue``` и обратимся к информации узла ```bleen```. <br/>
Информацией, которая хранится в узле клиента может быть ```url``` адрес клиента, либо любая другая информация требуемая для работы распределённого приложения. <br/>
Теперь эмулируем аварийное отключение любого клиента. Нажмем сочетание клавиш ```Ctrl + D``` в одной из консолей, создавшей эфимерный узел. <br/>
Проверим, что соответствующий узел пропал из ```mygroup```. Изменение списка дочерних узлов может произойти не сразу — от 2 до 20 ```tickTime```, значение которого можно посмотреть в ```zoo.cfg```.
<br/>
<img src="/LR4/images/bleen.png"> <br/>
Таким образом клиенты могут получать информацию о появлении и отключении других клиентов.
В заключении удалим узел ```/mygroup```.
<br/>
<img src="/LR4/images/delete.png"> <br/>
<br/>

### Пример управления конфигурацией распределённого приложения
Хранение конфигурационной информации в ZooKeeper одно из наиболее популярных приложений. Мы будем эмулировать данную концепцию также с помощью CLI. <br/>
Использование ZooKeeper для хранения конфигурационной информации имеет два преимущества: 
+ Новые клиенты могут узнавать конфигурацию кластера и определять свою роль самостоятельно,
+ Возможность подписки на обновление конфигурационных параметров. Это позволяет динамически реагировать на изменения в конфигурации во время выполнения, что необходимо в режиме работы 24/7. <br/>


Создадим в корне узел ```"myconfig"```, в задачу которого будет входить хранение конфигурации. В нашем случае узел будет хранить строку ```'sheep_count=1'```.
Во всех случаях, когда конфигурационная информация нашего гипотетического распределённого приложения будет изменяться, мы будем обновлять ```znode``` строкой с новым значением. Другим клиентам распределённого приложения достаточно проверять хранимые в этом узле данные. <br/>
Откроем новую консоль и подключитесь к ZooKeeper. Данная консоль будет играть роль физического сервера, который ожидает получить оповещение в случае изменения конфигурационной информации, записанной в ```/myconfig``` znode. <br/>
Следующая команда устанавливает watch-триггер на узел: <br/>
```cmd 
get -s -w /myconfig
``` 

</br>

Вернемся к первому терминалу и изменим значение ```myconfig```:
```cmd
set /myconfig 'sheep_count=2'
```
<br/>
Во втором терминале должно появиться оповещение об изменении данных! <br/>
<img src="/LR4/images/watcher.png"> <br/>


Триггер сбрасывается после одного срабатывания, а значит его придётся 'взводить' каждый раз заново. Как правило, в приложении, в логике обработчика события присутствует такая процедура. <br/>

Удалим узел ```/myconfig```и проверим, что эта команда выполнилась.
<img src="/LR4/images/deleteNode.png"> <br/>

## Разработка, запуск и проверка работы приложения


Monkey:
<img src="/LR4/images/monkey.png"> <br/>
Tiger:
<img src="/LR4/images/tiger.png"> <br/>


### Философы

Результат:
```
Philosopher 0 is going to eat
Philosopher 4 is going to eat
Philosopher 1 is going to eat
Philosopher 2 is going to eat
Philosopher 3 is going to eat
Philosopher 2 picked up the left fork
Philosopher 2 picked up the right fork
Philosopher 5 picked up the left fork
Philosopher 5 picked up the right fork
Philosopher 4 picked up the left fork
Philosopher 5 put the right fork
Philosopher 5 put the loft fork and finished eating
Philosopher 4 picked up the right fork
Philosopher 5 is thinking
Philosopher 1 picked up the left fork
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 1 picked up the right fork
Philosopher 3 picked up the left fork
Philosopher 2 is thinking
Philosopher 4 put the right fork
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 4 put the loft fork and finished eating
Philosopher 3 picked up the right fork
Philosopher 1 is thinking
Philosopher 4 is thinking
Philosopher 4 is going to eat
Philosopher 5 picked up the left fork
Philosopher 5 picked up the right fork
Philosopher 1 is going to eat
Philosopher 2 picked up the left fork
Philosopher 5 put the right fork
Philosopher 5 put the loft fork and finished eating
Philosopher 1 picked up the left fork
Philosopher 5 is thinking
Philosopher 3 is going to eat
Philosopher 3 put the right fork
Philosopher 3 put the loft fork and finished eating
Philosopher 2 picked up the right fork
Philosopher 3 is thinking
Philosopher 4 picked up the left fork
Philosopher 4 picked up the right fork
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 1 picked up the right fork
Philosopher 2 is thinking
Philosopher 2 is going to eat
Philosopher 3 picked up the left fork
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 1 is thinking
Philosopher 4 put the right fork
Philosopher 4 put the loft fork and finished eating
Philosopher 3 picked up the right fork
Philosopher 4 is thinking
Philosopher 3 put the right fork
Philosopher 3 put the loft fork and finished eating
Philosopher 3 is thinking
```

### Двуфазный коммит протокол для high-available регистра
Результат:

```
Waiting others clients: []
Client 1 request commit
Client 0 request rollback
Client 2 request rollback
Client 4 request commit
Client 3 request commit
Check clients
Client 0 do commit
Client 1 do commit
Client 2 do commit
Client 3 do commit
Client 4 do commit
Waiting others clients: []
```
