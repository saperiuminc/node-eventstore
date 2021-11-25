FROM node:16
ENV NODE_ENV=production

ARG NPM_TOKEN
ENV NPM_TOKEN=${NPM_TOKEN}

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
RUN chmod +x /wait

WORKDIR /app


COPY ["package.json", "package-lock.json*", "./"]

RUN echo "//registry.npmjs.org/:_authToken=$NPM_TOKEN" > .npmrc && \
    npm ci && \
    rm -f .npmrc

RUN npm install --production

COPY . .

CMD [ "npm", "start" ]