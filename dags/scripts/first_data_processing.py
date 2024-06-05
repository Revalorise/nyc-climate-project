# unzip, remove, rename operations
import zipfile


class DataProcessor:
    @staticmethod
    def unzip_data(source: str, destination: str) -> None:
        """
        Unzip the contents of a source file to a destination directory.
        """
        with zipfile.ZipFile(source, 'r') as zip_file:
            zip_file.extractall(destination)

    @staticmethod
    def remove_file(file_path: str) -> None:
        """
        Remove a file from the file system.
        """
        import os
        if os.path.exists(file_path):
            os.remove(file_path)

    @staticmethod
    def rename_file(old_name: str, new_name: str) -> None:
        """
        Rename a file in the file system.
        """
        import os
        if os.path.exists(old_name):
            os.rename(old_name, new_name)
