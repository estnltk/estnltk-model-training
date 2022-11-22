from random import sample, choices

class SpanSampler:
    
    def __init__(self, storage, collection, layer, attribute):
        self.storage = storage
        self.conn = storage.conn
        self.cur = self.conn.cursor()
        self.collection = collection
        self.layer = layer
        self.attribute = attribute
        self.attribute_locations_creation()
    
    def __call__(self, count, attribute, return_index=False, with_replacement=True): 
        # Returns iterator of type Text, Span or int, Text, span 
        # count determines the number of samples
        # with replacement means that same span casn be sampled several times
        self.conn.commit()
        self.create_sampling_matrix(attribute)
        indices = self.find_sampled_indices(count,with_replacement)
        result_list = []
        for idx in indices:
            if return_index:
                result_list.append((idx[0],self.collection[idx[1]],self.collection[idx[1]][self.layer][idx[2]]))
            else:
                result_list.append((self.collection[idx[1]],self.collection[idx[1]][self.layer][idx[2]]))
        self.clear_sampling_matrix()
        return result_list
    
    def attribute_locations_creation(self):
        self.conn.commit()
        self.cur.execute("""SELECT EXISTS (
           SELECT FROM information_schema.tables 
           WHERE  table_schema = 'public'
           AND    table_name   = 'attribute_locations'
           );""")
        res = self.cur.fetchall()
        if not res[0][0]:
            self.cur.execute("CREATE TABLE attribute_locations (layer_id integer, attribute_value varchar, indices integer[], count integer);")
            self.conn.commit()
            for i, txt in enumerate(self.collection):
                for tag in set(txt[self.layer][self.attribute]):
                    indices = [i for i, nertag in enumerate(txt[self.layer][self.attribute]) if nertag == tag]
                    self.cur.execute("INSERT INTO attribute_locations (layer_id, attribute_value,indices,count) VALUES (%s, %s, %s, %s)",(i, tag, indices,len(indices)))
        self.conn.commit()
        
    def create_sampling_matrix(self,attribute_val):
        self.cur.execute("CREATE TABLE sampling_matrix (id serial, layer integer, layer_index integer);")
        self.cur.execute("INSERT INTO sampling_matrix (layer,layer_index) (SELECT layer_id as layer, unnest(indices) as layer_index FROM attribute_locations WHERE attribute_value = '" + attribute_val + "');")
        self.conn.commit()
    
    def find_sampled_indices(self,count,with_replacement):
        self.cur.execute("SELECT * FROM sampling_matrix;")
        self.conn.commit()
        self.cur.execute("SELECT COUNT(*) FROM sampling_matrix;")
        span_count = self.cur.fetchall()[0][0]
        self.conn.commit()
        if with_replacement:
            sampled = choices(range(span_count),k=count)
        else:
            sampled = sample(range(span_count),count)
        self.cur.execute("SELECT * FROM sampling_matrix WHERE id IN " + str(tuple(sampled)) + ';')
        return self.cur.fetchall()
    
    def clear_sampling_matrix(self):
        self.cur.execute("DROP TABLE sampling_matrix;")
        self.conn.commit()
        
