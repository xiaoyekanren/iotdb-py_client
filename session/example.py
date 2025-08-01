# coding=utf-8
from interface import TreeSessionClient


def main():
    client = TreeSessionClient(
        ip='127.0.0.1',
        port=6667,
    )

    abc = client.query('show cluster')
    while abc.has_next():
        print(abc.next())


if __name__ == '__main__':
    main()




