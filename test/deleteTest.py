import ursa
import ray
ray.init()

manager = ursa.Graph_manager()
manager.create_graph("Test Graph")
manager.insert("Test Graph", "Key1", "Data1", foreign_keys = {'FK1':'test test test'})
manager.insert("Test Graph", "Key2", "Data2")
# assert ray.get(manager.select_row("Test Graph")) == {'Key2': 'Data2', 'Key1': 'Data1'}
# assert ray.get(manager.select_local_keys("Test Graph")) == {'Key2': set([]), 'Key1': set([])}
# assert ray.get(manager.select_foreign_keys("Test Graph")) == {'Key2': {}, 'Key1': {}}
manager.delete_row("Test Graph", "Key1")
# assert ray.get(manager.select_row("Test Graph")) == {'Key2': 'Data2'}
# assert ray.get(manager.select_local_keys("Test Graph")) == {'Key2': set([])}
# assert ray.get(manager.select_foreign_keys("Test Graph")) == {'Key2': {}}
