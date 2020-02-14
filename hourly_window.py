import datetime
from .subscriber import Subscriber
from .publisher import Publisher

def action():
    pass

def main():
    subscriber=Subscriber()
    subscriber.subscribe('/queue/preprocess/cleanup', action)
    pass


if __name__ == '__main__':
    main()