#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re

def punctuation(sent):
    return sent.replace("\\n", " ").replace("\\t", " ").replace("#", " ")

def emoji(sent):
    emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', sent)

def space(sent):
    space_pattern = re.compile('\s+')   # all space regular expression [ \t\n\r\f\v].
    return space_pattern.sub(r' ', sent)

class PPC:
    def __init__(self, sent):
        self.sent = sent
        self.length = len(sent)

    def combination(self):
        return( space(punctuation(emoji(self.sent))) )





def Make_Cooccurance_Matrix(list_sent):
    for sent in list_sent:
        print(sent.strip())
