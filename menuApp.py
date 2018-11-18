# GUI Requirements
from kivy.app import App
from kivy.lang import Builder
from kivy.core.window import Window

from kivy.uix.boxlayout import BoxLayout
from kivy.uix.screenmanager import ScreenManager, Screen

Builder.load_string("""
<NetworkScreen>:  
    BoxLayout:
        orientation: 'vertical'

        TextInput:   
            id: net_name

        Button:
            text: 'Setup Local Network'
            on_release: 

                import itertools;
                import netifaces;
                ip = netifaces.ifaddresses(net_name.text)[netifaces.AF_INET6][0]['addr'];
                
                from kivy.cache import Cache
                Cache.register('cache', limit=2, timeout=600)
                Cache.append('cache', 'ip', ip); 
                Cache.append('cache', 'count', itertools.count())
                root.manager.current = 'local'
        
        Button:
            text: 'Setup Distributed Network'
            on_release: 

                import itertools;
                import netifaces;
                ip = netifaces.ifaddresses(net_name.text)[netifaces.AF_INET6][0]['addr'];
                
                from kivy.cache import Cache
                Cache.register('cache', limit=2, timeout=600)
                Cache.append('cache', 'ip', ip); 
                Cache.append('cache', 'count', itertools.count())
                root.manager.current = 'distributed'


<LocalNetworkScreen>:
    BoxLayout:
        orientation: 'vertical'
            
        BoxLayout:
            orientation: 'horizontal'
            Label: 
                text:'Number of hosts'

            TextInput:   
                id: n_hosts

        Button:
            text: 'Add Host'
            on_release: 
                from kivy.cache import Cache
                import os,itertools,random;
                operations = ['+', '-', '*', '/']
                hosts_str = ""
                for i in range(int(n_hosts.text)): hosts_str += ('{}:{} '.format(Cache.get('cache', 'ip'), 50000 + i*2 + 1))
                for i, host in enumerate(range(int(n_hosts.text))):os.system('gnome-terminal --command="python3 ./host.py {} {} {} {} {}"'.format(50000 + i*2 ,Cache.get('cache', 'ip'), random.choice(operations), random.randrange(1,10,1), (' ').join([x for j,x in enumerate(hosts_str.split(' ')) if j!=i])))
                
        Button:
            text: 'Change Network'
            on_press: 
                root.manager.current = 'network';

<DistributedNetworkScreen>:

    BoxLayout:
        orientation: 'vertical'
            
        BoxLayout:
            orientation: 'horizontal'
            Label: 
                text:'Number of hosts in this machine'

            TextInput:   
                id: n_hosts

        BoxLayout:
            orientation: 'horizontal'
            Label: 
                text:'Number of hosts on other machine'

            TextInput:   
                id: n_hosts_other

        BoxLayout:
            orientation: 'horizontal'
            Label: 
                text:'IP of other machine'

            TextInput:   
                id: ip_other

        Button:
            text: 'Add Host'
            on_release: 
                from kivy.cache import Cache
                import os,itertools,random;
                operations = ['+', '-', '*', '/']
                hosts_str = ""
                partners_str = ""
                print(Cache.get('cache','ip'))
                for i in range(int(n_hosts.text)): hosts_str += ('{}:{} '.format(Cache.get('cache', 'ip'), 50000 + i*2 + 1))
                for i in range(int(n_hosts_other.text)): partners_str += ('{}:{} '.format(ip_other.text, 50000 + i*2 + 1))
                for i, host in enumerate(range(int(n_hosts.text))):os.system('gnome-terminal --command="python3 ./host.py {} {} {} {} {} {}"'.format(50000 + i*2 ,Cache.get('cache', 'ip'), random.choice(operations), random.randrange(1,10,1), (' ').join([x for j,x in enumerate(hosts_str.split(' ')) if j!=i])), partners_str.split(' ') )
                
        Button:
            text: 'Change Network'
            on_press: 
                root.manager.current = 'network';

""")

class NetworkScreen(Screen):
    def on_pre_enter(self):
        Window.size = (200,100)

class LocalNetworkScreen(Screen):
    def on_pre_enter(self):
        Window.size = (300, 150)     

class DistributedNetworkScreen(Screen):
    def on_pre_enter(self):
        Window.size = (500, 200) 

sm = ScreenManager()
sm.add_widget(NetworkScreen(name='network'))
sm.add_widget(LocalNetworkScreen(name='local'))
sm.add_widget(DistributedNetworkScreen(name='distributed'))

class MenuApp(App):

    def build(self):
        return sm

if __name__ == '__main__':
    MenuApp().run()