# Fetch_Rewards_DataEngineer
Ans1.We can construct a Dockerfile for this application and deploy it using any container management system for production deployment; with AWS, we may use ECS.

Ans2.Refactoring the code to make it more readable, Writing the code in a class and Using a better crypto library to mask the PII data.

Ans3.A variety of techniques are needed to scale a Python program with a rising dataset, including:
Data compression: Data can be compressed to be smaller, either in the database or during transmission, using less storage and bandwidth.
Horizontal scaling: Increase the number of machines supporting the program to handle the added load, either by adding extra application servers or database servers.
We will always have several messages in the SQS queue due to the expanding dataset of user logins, thus we must set a restriction on data transfer between SQS and compute and maintain it at a high number of messages just below the network transfer limits in order to process messages efficiently.

Ans4. In order to keep PII, We would need to keep the data in secure position.

Ans5.I have used hashlib for masking the values, prefered use python for 
this home test because I am familiar with it.
