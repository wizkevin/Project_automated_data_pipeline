import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="CLI to create data pipeline")
parser.add_argument('messages_path', help='path to messages data file')
parser.add_argument('users_path',help='path to user data file')
parser.add_argument('output_path', help='path to output the result of the script')

args = parser.parse_args()

def create_pipeline(messages_path, users_path, output_path):
    messages_df = pd.read_csv(messages_path, sep=",")
    user_df = pd.read_csv(users_path, sep=',')
    
    user_id_list = [index for index in user_df['user_id']]
    author_id_list = [index for index in messages_df['author_id']]
    user_first_name_list = [name for name in user_df['first_name']]
    user_last_name_list = [name for name in user_df['last_name']]
    number_of_messages_list = [author_id_list.count(index) for index in user_id_list]
    
    dict_out = {
        "user_id": user_id_list,
        "first_name": user_first_name_list,
        "last_name": user_last_name_list,
        "number_of_messages": number_of_messages_list
    }
    
    pipeline_df = pd.DataFrame(dict_out).sort_values("number_of_messages")
    
    pipeline_df.to_csv(f"{output_path}\pipeline_result.csv", index=False)
    
    return None

try:
    create_pipeline(args.messages_path, args.users_path, args.output_path)
except FileNotFoundError:
    print("Vérifiez les chemins d'accès aux différents fichiers")