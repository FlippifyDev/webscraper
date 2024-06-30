class BatchedQueue:
    def __init__(self, queue_items, batch_size) -> None:
        self.queue_items = queue_items
        self.batch_size = batch_size
        self.queue = self.__create_queue_batches()
        self.length = len(self.queue)
        self.size = self.length
        
        self.batch_number = 1


    def pop(self):
        # Pop each batch from the queue
        self.batch_number += 1
        if self.queue:
            item = self.queue[0]
            self.queue = self.queue[1:]
            self.length -= 1
            return item
        else:
            return None


    def __create_queue_batches(self, start = 0):
        # Create a list of lists where each sublist is a batch
        end = start + self.batch_size
        batch = self.queue_items[start:end]
        if not batch:
            return []
        return [batch] + self.__create_queue_batches(end)
    
        
    def __str__(self):
        return str(self.queue)