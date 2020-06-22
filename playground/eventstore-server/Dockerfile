FROM node:12-slim as builder

ARG NODE_ENV=development
ENV NODE_ENV=${NODE_ENV}

# NOTE: install apks here
COPY package*.json ./
RUN npm install -g npm-cli-login
RUN npm-cli-login -u saperium -p MakeItHappen2016 -e saperium.rocks@gmail.com
RUN npm install

FROM node:12-slim

# NOTE: install runtime apks here
RUN apt-get update && apt-get install -y netcat

# From here we load our application's code in, therefore the previous docker
# "layer" thats been cached will be used if possible
WORKDIR /app
COPY --from=builder node_modules node_modules
COPY . /app

EXPOSE 3000

CMD ["npm", "start"]