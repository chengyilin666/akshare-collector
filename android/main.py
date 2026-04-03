#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare A股数据采集器 - Kivy安卓版
"""

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.scrollview import ScrollView
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.metrics import dp

import os


class MenuButton(Button):
    def __init__(self, **kwargs):
        super(MenuButton, self).__init__(**kwargs)
        self.font_size = dp(16)
        self.size_hint_y = None
        self.height = dp(50)
        self.background_color = (0.2, 0.4, 0.8, 1)
        self.color = (1, 1, 1, 1)


class MainScreen(BoxLayout):
    def __init__(self, **kwargs):
        super(MainScreen, self).__init__(**kwargs)
        self.orientation = 'vertical'
        self.padding = dp(10)
        self.spacing = dp(5)
        
        title = Label(
            text='AKShare A股数据采集器',
            font_size=dp(24),
            size_hint_y=None,
            height=dp(60),
            color=(0.2, 0.6, 1, 1)
        )
        self.add_widget(title)
        
        status = Label(
            text='准备就绪',
            font_size=dp(14),
            size_hint_y=None,
            height=dp(30),
            color=(0.9, 0.9, 0.9, 1)
        )
        self.add_widget(status)
        
        scroll = ScrollView()
        menu_layout = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=dp(5)
        )
        menu_layout.bind(minimum_height=menu_layout.setter('height'))
        scroll.add_widget(menu_layout)
        self.add_widget(scroll)
        
        items = [
            ('系统检查', self.check_system),
            ('关于', self.show_about),
        ]
        
        for text, callback in items:
            btn = MenuButton(text=text)
            btn.bind(on_press=callback)
            menu_layout.add_widget(btn)
        
        info = Label(
            text='数据目录: /sdcard/akshare_data',
            font_size=dp(12),
            size_hint_y=None,
            height=dp(30),
            color=(0.6, 0.6, 0.6, 1)
        )
        self.add_widget(info)
    
    def check_system(self, instance):
        content = Label(
            text='系统状态: 正常\n\n这是简化版安卓应用\n完整功能请使用PC端:\npython run_collector.py',
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='系统检查',
            content=content,
            size_hint=(0.9, 0.6),
            auto_dismiss=True
        )
        popup.open()
    
    def show_about(self, instance):
        content = Label(
            text='AKShare A股数据采集器\n\n版本: 1.0.0\n\n功能:\n- 股票数据采集\n- 日线数据存储\n- 资金流向分析',
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='关于',
            content=content,
            size_hint=(0.9, 0.6),
            auto_dismiss=True
        )
        popup.open()


class AKShareApp(App):
    def build(self):
        self.title = 'AKShare A股采集器'
        return MainScreen()
    
    def on_pause(self):
        return True
    
    def on_resume(self):
        pass


if __name__ == '__main__':
    AKShareApp().run()
