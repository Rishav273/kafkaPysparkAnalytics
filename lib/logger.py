import logging

class Logger:

    """
        Logger class to log information both to the console and to a specified file.
    """

    def __init__(self, name, log_file, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # create a file handler to log messages to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s'))

        # create a stream handler to log messages to the console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(name)s:%(levelname)s: %(message)s'))

        # add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)


    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)


if __name__ == "__main__":
    logger = Logger('SparkStreamingApp', 'log.txt', level=logging.INFO)

    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
    logger.critical('This is a critical message')


