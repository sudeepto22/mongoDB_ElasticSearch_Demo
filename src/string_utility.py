class StrToCamelCase:
    def __init__(self, original_str: str):
        self.original_str = original_str

    def change_string(self) -> str:
        """
        Convert underscore separated string to camelCase

        :return camel_case_str: str
        """
        camel_case_str: str = ""
        flag = False
        for i in self.original_str:
            if i != "_":
                if flag:
                    camel_case_str += i.upper()
                else:
                    camel_case_str += i
                flag = False
            else:
                flag = True
        return camel_case_str


class StringManipulate:
    def __init__(self, original_str: str):
        self.original_str = original_str

    def change_string(self) -> str:
        """
        Convert a string with underscore separated

        :return manipulated_str: str
        """
        manipulated_str: str = ""
        flag = True
        for i in self.original_str:
            if i == " " or not i.isalpha():
                if flag:
                    manipulated_str += "_"
                    flag = False
            elif i.isalpha():
                manipulated_str += i.lower()
                flag = True
        if manipulated_str.startswith("_"):
            manipulated_str = manipulated_str[1:]
        if manipulated_str.endswith("_"):
            manipulated_str = manipulated_str[:-1]
        return manipulated_str


