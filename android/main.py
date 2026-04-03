#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare Stock Data Collector - Kivy Android Version
"""

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.scrollview import ScrollView
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.uix.spinner import Spinner
from kivy.metrics import dp
from kivy.clock import Clock

import os
import sys

DATA_DIR = '/sdcard/akshare_data'


class MenuButton(Button):
    def __init__(self, **kwargs):
        super(MenuButton, self).__init__(**kwargs)
        self.font_size = dp(18)
        self.size_hint_y = None
        self.height = dp(55)
        self.background_color = (0.2, 0.5, 0.8, 1)
        self.color = (1, 1, 1, 1)
        self.bold = True


class StatusLabel(Label):
    def __init__(self, **kwargs):
        super(StatusLabel, self).__init__(**kwargs)
        self.font_size = dp(14)
        self.size_hint_y = None
        self.height = dp(40)
        self.color = (0.3, 0.8, 0.3, 1)
        self.halign = 'center'


class MainScreen(BoxLayout):
    def __init__(self, **kwargs):
        super(MainScreen, self).__init__(**kwargs)
        self.orientation = 'vertical'
        self.padding = dp(15)
        self.spacing = dp(8)
        
        self.status_label = None
        self.build_ui()
    
    def build_ui(self):
        title = Label(
            text='AKShare Collector',
            font_size=dp(28),
            size_hint_y=None,
            height=dp(70),
            color=(0.1, 0.6, 0.9, 1),
            bold=True
        )
        self.add_widget(title)
        
        subtitle = Label(
            text='A-Share Stock Data Collector',
            font_size=dp(14),
            size_hint_y=None,
            height=dp(30),
            color=(0.6, 0.6, 0.6, 1)
        )
        self.add_widget(subtitle)
        
        self.status_label = StatusLabel(text='Ready')
        self.add_widget(self.status_label)
        
        scroll = ScrollView()
        menu_layout = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=dp(8),
            padding=dp(5)
        )
        menu_layout.bind(minimum_height=menu_layout.setter('height'))
        scroll.add_widget(menu_layout)
        self.add_widget(scroll)
        
        items = [
            ('System Check', self.check_system, (0.2, 0.6, 0.3, 1)),
            ('Stock List', self.collect_stock_list, (0.8, 0.4, 0.2, 1)),
            ('Daily Quotes', self.collect_daily, (0.8, 0.5, 0.2, 1)),
            ('Fund Flow', self.collect_fund_flow, (0.8, 0.6, 0.2, 1)),
            ('Realtime Data', self.collect_realtime, (0.7, 0.3, 0.3, 1)),
            ('Data Status', self.show_data_status, (0.3, 0.5, 0.8, 1)),
            ('Settings', self.show_settings, (0.5, 0.5, 0.5, 1)),
            ('About', self.show_about, (0.4, 0.4, 0.4, 1)),
        ]
        
        for text, callback, color in items:
            btn = MenuButton(text=text)
            btn.background_color = color
            btn.bind(on_press=callback)
            menu_layout.add_widget(btn)
        
        info = Label(
            text='Data Dir: /sdcard/akshare_data',
            font_size=dp(12),
            size_hint_y=None,
            height=dp(30),
            color=(0.5, 0.5, 0.5, 1)
        )
        self.add_widget(info)
    
    def update_status(self, text):
        if self.status_label:
            self.status_label.text = text
    
    def check_system(self, instance):
        self.update_status('Checking system...')
        
        status_items = []
        
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            status_items.append('[OK] Storage Access')
        except Exception as e:
            status_items.append(f'[FAIL] Storage: {str(e)}')
        
        try:
            import duckdb
            status_items.append('[OK] DuckDB')
        except:
            status_items.append('[FAIL] DuckDB (need PC)')
        
        try:
            import akshare
            status_items.append('[OK] AKShare')
        except:
            status_items.append('[FAIL] AKShare (need PC)')
        
        status_items.append('\nNote: Full features require PC')
        status_items.append('Run: python run_collector.py')
        
        content = Label(
            text='\n'.join(status_items),
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='System Status',
            content=content,
            size_hint=(0.9, 0.7),
            auto_dismiss=True
        )
        popup.open()
        self.update_status('Ready')
    
    def collect_stock_list(self, instance):
        self.show_message('Stock List', 'This feature requires PC.\n\nRun on PC:\npython collector_static.py stock_list')
    
    def collect_daily(self, instance):
        self.show_message('Daily Quotes', 'This feature requires PC.\n\nRun on PC:\npython collector_daily.py stock')
    
    def collect_fund_flow(self, instance):
        self.show_message('Fund Flow', 'This feature requires PC.\n\nRun on PC:\npython collector_daily.py fund_flow')
    
    def collect_realtime(self, instance):
        self.show_message('Realtime Data', 'This feature requires PC.\n\nRun on PC:\npython collector_realtime.py all')
    
    def show_data_status(self, instance):
        self.update_status('Checking data...')
        
        info = []
        info.append('Data Directory:')
        info.append(f'  {DATA_DIR}')
        info.append('')
        
        if os.path.exists(DATA_DIR):
            files = os.listdir(DATA_DIR)
            db_files = [f for f in files if f.endswith('.duckdb') or f.endswith('.db')]
            if db_files:
                info.append('Database Files:')
                for f in db_files[:5]:
                    size = os.path.getsize(os.path.join(DATA_DIR, f))
                    info.append(f'  {f}: {size/1024/1024:.1f} MB')
            else:
                info.append('No database files found')
                info.append('')
                info.append('Run on PC first to collect data')
        else:
            info.append('Directory not found')
            info.append('Run on PC first to collect data')
        
        content = Label(
            text='\n'.join(info),
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='Data Status',
            content=content,
            size_hint=(0.9, 0.7),
            auto_dismiss=True
        )
        popup.open()
        self.update_status('Ready')
    
    def show_settings(self, instance):
        content = Label(
            text='Settings\n\n'
                 'Version: 2.0.0\n'
                 'Storage: /sdcard/akshare_data\n'
                 'Database: DuckDB + SQLite\n\n'
                 'Note:\n'
                 'This is a viewer app.\n'
                 'Use PC version for data collection.',
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='Settings',
            content=content,
            size_hint=(0.9, 0.6),
            auto_dismiss=True
        )
        popup.open()
    
    def show_about(self, instance):
        content = Label(
            text='AKShare Stock Data Collector\n\n'
                 'Version: 2.0.0\n\n'
                 'Features:\n'
                 '- A-Share stock data collection\n'
                 '- Daily quotes storage\n'
                 '- Fund flow analysis\n'
                 '- Realtime monitoring\n\n'
                 'PC Version:\n'
                 'python run_collector.py\n\n'
                 'GitHub:\n'
                 'github.com/chengyilin666/akshare-collector',
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title='About',
            content=content,
            size_hint=(0.9, 0.8),
            auto_dismiss=True
        )
        popup.open()
    
    def show_message(self, title, message):
        content = Label(
            text=message,
            font_size=dp(14),
            size_hint_y=None
        )
        content.bind(
            width=lambda *x: content.setter('text_size')(content, (content.width, None)),
            texture_size=lambda *x: content.setter('height')(content, content.texture_size[1])
        )
        
        popup = Popup(
            title=title,
            content=content,
            size_hint=(0.9, 0.6),
            auto_dismiss=True
        )
        popup.open()


class AKShareApp(App):
    def build(self):
        self.title = 'AKShare Collector'
        return MainScreen()
    
    def on_pause(self):
        return True
    
    def on_resume(self):
        pass


if __name__ == '__main__':
    AKShareApp().run()
