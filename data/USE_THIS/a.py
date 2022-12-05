
# for i in ['id', 'author', 'body', 'created_utc', 'is_submitter', 'link_id',
#     'score', 'subreddit', 'subreddit_id', 'Toxicity', 'Insult',
#     'Severe_Toxicity', 'Identity_Attack', 'Profanity']:
#     print("$comments_data['data']['" + i + "'] = $_POST['" + i + "'];")


# for i in ['id', 'author', 'created_utc', 'full_link', 'num_comments', 'num_crossposts', 'score', 'selftext',
# 'subreddit', 'subreddit_id', 'title', 'url', 'Toxicity', 'Insult',
# 'Severe_Toxicity', 'Identity_Attack', 'Profanity']:
#     print("'" + i + "' => $" + i)


# # for i in ['id', 'author', 'body', 'created_utc', 'is_submitter', 'link_id',
# #     'score', 'subreddit', 'subreddit_id', 'Toxicity', 'Insult',
# #     'Severe_Toxicity', 'Identity_Attack', 'Profanity']:
# #     print("public $" + i + ";")

# # ['id', 'author', 'created_utc', 'full_link', 'is_self', 'is_video',
# #        'num_comments', 'num_crossposts', 'over_18', 'score', 'selftext',
# #        'subreddit', 'subreddit_id', 'title', 'url']

# for i in ['id', 'author', 'created_utc', 'full_link', 'num_comments', 'num_crossposts', 'score', 'selftext',
# 'subreddit', 'subreddit_id', 'title', 'url', 'Toxicity', 'Insult',
# 'Severe_Toxicity', 'Identity_Attack', 'Profanity']:
#     print("public $" + i + ";")




for i in ['id', 'author', 'created_utc', 'full_link', 'num_comments', 'num_crossposts', 'score', 'selftext',
'subreddit', 'subreddit_id', 'title', 'url', 'Toxicity', 'Insult',
'Severe_Toxicity', 'Identity_Attack', 'Profanity']:
    print("'" + i + "' : processed_df_posts.iloc[count]['" + i + "'],")
