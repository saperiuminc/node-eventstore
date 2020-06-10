FROM node:12-alpine as builder

ARG NODE_ENV=development
ENV NODE_ENV=${NODE_ENV}

# use changes to package.json to force Docker not to use the cache
# when we change our application's nodejs dependencies:
RUN apk add --no-cache python make g++

COPY package*.json ./
RUN npm install

FROM node:12-alpine

### FROM DF_MICROSERVICE_BASE ###
RUN apk add --no-cache netcat-openbsd openjdk7-jre-base bash redis

# From here we load our application's code in, therefore the previous docker
# "layer" thats been cached will be used if possible
WORKDIR /app
COPY --from=builder node_modules node_modules
COPY . /app

EXPOSE 3000

CMD ["npm", "start"]