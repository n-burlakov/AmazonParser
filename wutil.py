import logging
import coloredlogs
from logging import handlers

import os


def get_logger(name=None, level=logging.DEBUG):
    logger = logging.getLogger(name)
    name = 'root' if name is None else name
    if not os.path.exists(os.getcwd() + '/logs'):
        os.makedirs(os.getcwd() + '/logs')
    logger.setLevel(level)
    logger.propagate = True
    logger_Handler = handlers.TimedRotatingFileHandler(filename=f"{os.getcwd()+'/logs'}/{name}.log")
    logger_Formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logger_Handler.setFormatter(logger_Formatter)
    logger.addHandler(logger_Handler)
    return logger

#//  "cookie": "csm-sid=724-8933834-7312163; x-amz-captcha-1=1702905843867542; x-amz-captcha-2=ruZRinv4ZWb8o/OfP8DMfg==; session-id=135-5930044-6539068; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn='L5Z9:FR'; ubid-main=135-6066493-8355713; csm-hit=tb:RWEKE7DXQW3XA5K7FGS7+s-AHJTTK86WNW8WV7SHPD1|1702902728295&t:1702902728295&adb:adblk_no; session-token=OVqORo1u6jYAWWR4h1kj0754kAZDBHwDIMyfmB+oeUGjPDfJVXJ9Ikky6sB/gQ7pGDYTCwNThAck/alJe9faRJGBj4NjrIWD8uQBqH6DeF2XjCJlKFgY+/LzZgd+HSgbWhPYUf3/KOKSwSo4udaU4XvrbiNv2agT4kp0fu72hI4rH6iAOGm8nhf9sVEt50Ji8Kd8528BcHB2kjtSZWep1sq+3N2dZa/mcsdLgb6cnx1OEh1raoyHTZl/CHZdAf6wl2RC/JkPNuDMLtAXi5RUhUxF81phHH2rRs+CuO/2rGP8vg/sBc3P+8/hHHeIYCIaRQdvP8OhuFRgI8yDzgHqxlSqP4Y1Ph66",