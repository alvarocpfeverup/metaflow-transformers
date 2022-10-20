from metaflow import FlowSpec, step


class HelloWorldFlow(FlowSpec):

    @step
    def start(self):
        """First step in the flow"""
        print("First step")
        self.next(self.hello)

    @step
    def hello(self):
        print("Hello World!")
        self.next(self.end)

    @step
    def end(self):
        """Last step"""
        print("Goodbye World!")


if __name__ == "__main__":
    HelloWorldFlow()